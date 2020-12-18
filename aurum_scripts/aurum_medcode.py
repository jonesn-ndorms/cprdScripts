""" A script to create aurum snomed codelists from readcode lists using the CleansedReadCode field for a join """

import os
import pymysql as sql
import pandas as pd


READ_PATH = "Z:\\Big_Data\\Codes\\readcode\\arthritis\\"
WRITE_PATH = "C:\\users\\jonesn\\documents\\arthritis\\"

WRITE_REPORT = True
# The sort report functionality doesn't quite work yet. Keep false for now
SORT_REPORT = False
REPORT_LOCATION = "C:\\users\\jonesn\\documents\\data_reports\\"
REPORT_NAME = "arthritis_report.txt"

try:
    cnx = sql.connect(
        host="localhost",
        user="root",
        password="",
        db="cprd_codes"
    )

    cur = cnx.cursor()
    query = ("SELECT * FROM aurum_medcode")
    cur.execute(query)
    aurum_medcode = pd.DataFrame(cur.fetchall())
    aurum_medcode.columns = [desc[0] for desc in cur.description]
    aurum_medcode = aurum_medcode.astype(str)

    if WRITE_REPORT:
        report = open(REPORT_LOCATION + REPORT_NAME, 'w', encoding='Latin-1')
        report.write('Name\tConverted\tNot converted\tMissing readcodes\tDuplicated readcodes\n')
        report.close()

    for folder, dirs, files in os.walk(READ_PATH, topdown=True):
        dirs[:] = [d for d in dirs if d[:1] not in [".", "_"]]
        files[:] = [f for f in files if f[-4:] == ".txt" and f != "medcode_20200601.txt"]
        if not os.path.exists(WRITE_PATH + folder.replace(READ_PATH, "")):
            os.makedirs(WRITE_PATH + folder.replace(READ_PATH, ""))
        for f in files:
            df = pd.read_csv(folder + "\\" + f, sep="\t", usecols=['readcode',], dtype={'readcode': 'string'}).rename(columns={'readcode' : 'CleansedReadCode'})
            df = df.dropna()
            join = df.join(aurum_medcode.set_index("CleansedReadCode"), "CleansedReadCode", "inner", "a", "b")
            join = join.drop_duplicates()
            if join.MedCodeId.count() != 0:
                join.to_csv(WRITE_PATH + folder.replace(READ_PATH, "") + "\\" + f, sep="\t", index=False,
                    columns = ['MedCodeId','Term','OriginalReadCode','CleansedReadCode','SnomedCTConceptId','SnomedCTDescriptionId','Release','EmisCodeCategoryId']
                )
            if WRITE_REPORT:
                gold_file_count = df.CleansedReadCode.count()
                join_count = join.CleansedReadCode.count()
                missing = ';'.join(list(set(df.CleansedReadCode).difference(join.CleansedReadCode)))
                dupes = ';'.join(list(join[join.duplicated('CleansedReadCode', keep=False)].CleansedReadCode.unique()))
                report = open(REPORT_LOCATION + REPORT_NAME, 'a', encoding='Latin-1')
                report.write(f"{f}\t{str(join_count)}\t{str(int(gold_file_count - join_count))}\t{missing}\t{dupes}\n")
                report.close()
    if WRITE_PATH and SORT_REPORT:
        report = open(REPORT_LOCATION + "aurum_medcode_report.txt", 'r')
        report_array = report.readlines()
        report.close()
        report = open(REPORT_LOCATION + "aurum_medcode_report.txt", 'w', encoding='Latin-1')
        report.write(report_array[0] + report_array[1:].sort().join(''))
        report.close()

finally:   
    cnx.close()
