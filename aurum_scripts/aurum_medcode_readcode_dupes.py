""" Creates aurum snomed codelists from readcode lists using the CleansedReadCode field for a join, with additional code
    for dealing with duplicated raedcodes in the aurum dictionary """

import os
import pymysql as sql
import pandas as pd


READ_PATH = "Z:\\Big_Data\\Codes\\readcode\\"
WRITE_PATH = "Z:\\Big_Data\\Codes\\snomed_new\\"

WRITE_REPORT = True
# The sort report functionality doesn't quite work yet. Keep false for now
SORT_REPORT = False
REPORT_NAME = "aurum_medcode_report_alt.txt"
REPORT_LOCATION = "Z:\\Big_Data\\Codes\\snomed_new\\"

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
                dupes = join[join.duplicated('CleansedReadCode', keep=False)]
                #if len(dupes.MedCodeId)>0:
                #    for read in dupes.CleansedReadCode.unique():
                #        
                #else:
                #    pass
                #    join.to_csv(WRITE_PATH + folder.replace(READ_PATH, "") + "\\" + f, sep="\t", index=False,
                #        columns = ['MedCodeId','Term','OriginalReadCode','CleansedReadCode','SnomedCTConceptId','SnomedCTDescriptionId','Release','EmisCodeCategoryId']
                #    )
            if WRITE_REPORT:
                gold_file_count = df.CleansedReadCode.count()
                join_count = join.CleansedReadCode.count()
                missing = ';'.join(list(set(df.CleansedReadCode).difference(join.CleansedReadCode)))
                dupes = ';'.join(list(join[join.duplicated('CleansedReadCode', keep=False)].CleansedReadCode.unique()))
                report = open(REPORT_LOCATION + REPORT_NAME, 'a', encoding='Latin-1')
                report.write(f"{f}\t{str(join_count)}\t{str(int(gold_file_count - join_count))}\t{missing}\t{dupes}\n")
                report.close()
    if WRITE_PATH and SORT_REPORT:
        report = open(REPORT_LOCATION + REPORT_NAME, 'r')
        report_array = report.readlines()
        report.close()
        report = open(REPORT_LOCATION + REPORT_NAME, 'w', encoding='Latin-1')
        report.write(report_array[0] + report_array[1:].sort().join(''))
        report.close()

finally:   
    cur.close()
