""" A script to create gold medcode lists from snomed lists using the CleansedReadCode field for a join """

import os
import pymysql as sql
import pandas as pd


READ_PATH = "D:/icd10_codes_to_map/results/snomed"
WRITE_PATH = READ_PATH + "/readcode_conversions"

WRITE_REPORT = True
# The sort report functionality doesn't quite work yet. Keep false for now
SORT_REPORT = False
REPORT_LOCATION = WRITE_PATH

accepted_file_extensions = ['.csv']
excluded_files = ['gold_medcode_report.txt']

file_delimiter = ','
text_qualifier = '"'

try:
    cnx = sql.connect(
        host="localhost",
        user="root",
        password="",
        db="cprd_codes"
    )

    cur = cnx.cursor()
    query = ("SELECT s.SnomedCtConceptId, r.* FROM aurum_medcode as s inner join gold_medcode as r on s.CleansedReadCode = r.readcode")
    cur.execute(query)
    gold_medcode = pd.DataFrame(cur.fetchall())
    gold_medcode.columns = [desc[0] for desc in cur.description]
    gold_medcode = gold_medcode.astype(str)

    if WRITE_REPORT:
        report = open(REPORT_LOCATION + "/gold_medcode_report.txt", 'w', encoding='Latin-1')
        report.write('Name\tConverted\tNot converted\n')
        report.close()

    for folder, dirs, files in os.walk(READ_PATH, topdown=True):
        dirs[:] = [d for d in dirs if d[:1] not in [".", "_"]]
        files[:] = [f for f in files if f[-4:] in accepted_file_extensions and f not in excluded_files]
        if not os.path.exists(WRITE_PATH + folder.replace(READ_PATH, "")):
            os.makedirs(WRITE_PATH + folder.replace(READ_PATH, ""))
        for f in files:
            df = pd.read_csv(folder + "/" + f, sep=file_delimiter, quotechar=text_qualifier, usecols=['concept_code',], dtype={'concept_code': 'string'}).rename(columns={'concept_code' : 'SnomedCtConceptId'})
            df = df.dropna()
            join = df.join(gold_medcode.set_index("SnomedCtConceptId"), "SnomedCtConceptId", "inner", "a", "b")
            join = join.drop_duplicates()
            if join.medcode.count() != 0:
                join.to_csv(WRITE_PATH + folder.replace(READ_PATH, "") + "\\" + f, sep="\t", index=False,
                    columns = ['medcode','readcode','clinicalevents','immunisationevents','referralevents','testevents','readterm','databasebuild']
                )
            if WRITE_REPORT:
                gold_file_count = df.SnomedCtConceptId.count()
                join_count = join.SnomedCtConceptId.count()
                report = open(REPORT_LOCATION + "/gold_medcode_report.txt", 'a', encoding='Latin-1')
                report.write(f"{f}\t{str(join_count)}\t{str(int(gold_file_count - join_count))}\n")
                report.close()
    if WRITE_PATH and SORT_REPORT:
        report = open(REPORT_LOCATION + "/gold_medcode_report.txt", 'r')
        report_array = report.readlines()
        report.close()
        report = open(REPORT_LOCATION + "/gold_medcode_report.txt", 'w', encoding='Latin-1')
        report.write(report_array[0] + report_array[1:].sort().join(''))
        report.close()

finally:   
    cnx.close()
