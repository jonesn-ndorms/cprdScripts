""" Populates cura_db with codes and hierarchical structure of the prodcode folder
    WARNING: this is not yet set up for updating the database. It should only be used for a one-time setup. """

import os
import pymysql
import pandas as pd

try:
    cnx = pymysql.connect(
        host="localhost",
        user="root",
        password="",
        db="prodcode_db"
    )
    cnx.autocommit = True
    cur = cnx.cursor()
    cur.execute("truncate table cura_prodcode;")
    cur.execute("truncate table cura_prodlist;")
    cur.execute("truncate table cura_prodgroup;")

    path_string = "Z:\\Big_Data\\Codes\\prodcode"

    def files_to_db(path, father_id):
        query = ""
        if os.path.isdir(path):
            query = f"insert into cura_prodgroup (group_name, father_id) values ('{os.path.basename(path)}', {father_id})"
            cur.execute(query)
            query = "select max(group_id) from cura_prodgroup"
            cur.execute(query)
            group_id = cur.fetchone()[0]
            for x in os.listdir(path):
                if x[0] != "_" and x[0] != "." and os.path.splitext(x)[1] not in ['.zip', '.rar', '.7z']:
                    files_to_db(os.path.join(path, x), group_id)
        else:
            query = f"insert into cura_prodlist (list_name, group_id) values ('{os.path.basename(path)}', {father_id})"
            cur.execute(query)
            query = "select max(list_id) from cura_prodlist"
            cur.execute(query)
            list_id = cur.fetchone()[0]
            table = pd.read_csv(path, '\t', dtype=str, usecols=['prodcode', 'productname', 'drugsubstancename', 'substancestrength', 'formulation', 'routeofadministration']).fillna('')
            for row in table.to_numpy():
                query = f"insert into cura_prodcode values ({list_id}, {row[0]}, '" + row[1].replace('\'', '\\\'') + "', '" + row[2].replace('\'', '\\\'') 
                query += f"', '{row[3]}', '{row[4]}', '{row[5]}')"
                cur.execute(query)
        
    
    for file_name in os.listdir(path_string):
        fp = os.path.join(path_string, file_name)
        if file_name[0] == "_" or file_name[0] == "." or os.path.isfile(fp) and os.path.splitext(file_name)[1] not in ['.zip', '.rar', '.7z']:
            continue
        files_to_db(fp, 'null')


finally:
    cnx.close()