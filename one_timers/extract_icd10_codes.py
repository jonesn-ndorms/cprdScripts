""" Was used to extract the icd10 codes from a set of files and store them in a single table with their filenames. """

import os
import pymysql as sql
import pandas as pd

path = "D:/icd10_codes_to_map/"

files = os.listdir(path)

cnx = sql.connect(
    host="localhost",
    user="root",
    password = "",
    db = "cdm_vocabulary_53"
)
cnx.automcommit = True
cur = cnx.cursor()

i=0
for f in files:
    i+=1
    df = pd.read_csv(path+f, sep=',', quotechar='"', usecols=['ICD10 TERM', 'ICD10 CODE'])
    for row in df.to_numpy():
        query = f"insert into icd10_codes (id, filename, term, icd10) values ('{i}', '{f}', '" + row[0].replace("'", "''") + f"', '{row[1]}')"
        cur.execute(query)
cnx.commit()
cnx.close()