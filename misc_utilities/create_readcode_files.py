import pandas as pd
import os
import pymysql as sql

processing_folder = "D:/icd10_codes_to_map/infections/results/readcode/"
output_path = processing_folder + "output_codelists/"
files = [f for f in os.listdir(processing_folder) if f[-4:] == '.csv']

try:
    cnx = sql.connect(
        host="localhost",
        user="root",
        password="",
        db="cprd_codes"
    )
    cur = cnx.cursor()
    cur.execute('select * from gold_medcode')

    read_dic = pd.DataFrame(cur.fetchall())
    read_dic.columns = [desc[0] for desc in cur.description]
    read_dic = read_dic.astype(str)

    for f in files:
        df = pd.read_csv(processing_folder + f, sep=',', quotechar='"', usecols=['concept_code'], 
            dtype={'concept_code': str}).rename(columns={'concept_code': 'readcode'})
        df = df.dropna()
        join = df.join(read_dic.set_index('readcode'), 'readcode', 'inner', 'a', 'b')
        join = join.drop_duplicates()
        if join.readcode.count() > 0:
            join.to_csv(output_path + f, sep='\t', index=False,
                columns=['medcode','readcode','clinicalevents','immunisationevents','referralevents','testevents','readterm','databasebuild'])
finally:
    cnx.close()
