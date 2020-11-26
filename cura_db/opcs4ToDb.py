""" Populates cura_db with codes and hierarchical structure of the opcs4 folder
    WARNING: this is not yet set up for updating the database. It should only be used for a one-time setup. """

import os
import pymysql
import pandas as pd

try:
    cnx = pymysql.connect(
        host="localhost",
        user="root",
        password="",
        db="opcs4_db"
    )
    cnx.autocommit = True
    cur = cnx.cursor()
    cur.execute("truncate table cura_opcs4code;")
    cur.execute("truncate table cura_opcs4list;")
    cur.execute("truncate table cura_opcs4group;")

    path_string = "Z:\\Big_Data\\Codes\\opcs4"

    def files_to_db(path, father_id):
        query = ""
        if os.path.isdir(path):
            query = f"insert into cura_opcs4group (group_name, father_id) values ('{os.path.basename(path)}', {father_id})"
            cur.execute(query)
            query = "select max(group_id) from cura_opcs4group"
            cur.execute(query)
            group_id = cur.fetchone()[0]
            for x in os.listdir(path):
                if x[0] != "_" and x[0] != "." and os.path.splitext(x)[1] not in ['.zip', '.rar', '.7z']:
                    files_to_db(os.path.join(path, x), group_id)
        else:
            query = f"insert into cura_opcs4list (list_name, group_id) values ('{os.path.basename(path)}', {father_id})"
            cur.execute(query)
            query = "select max(list_id) from cura_opcs4list"
            cur.execute(query)
            list_id = cur.fetchone()[0]
            table = pd.read_csv(path, '\t').fillna('')
            for row in table.to_numpy():
                query = f"insert into cura_opcs4code values ({list_id}, '{row[0]}', '{row[1]}', '" + row[2].replace('\'', '\\\'') + "')"
                cur.execute(query)
        
    
    for file_name in os.listdir(path_string):
        fp = os.path.join(path_string, file_name)
        if file_name[0] == "_" or file_name[0] == "." or os.path.isfile(fp) and os.path.splitext(file_name)[1] not in ['.zip', '.rar', '.7z']:
            continue
        files_to_db(fp, 'null')


finally:
    cnx.close()