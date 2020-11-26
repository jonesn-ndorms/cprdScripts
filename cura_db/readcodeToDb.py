""" Populates cura_db with codes and hierarchical structure of the readcode folder
    WARNING: this is not yet set up for updating the database. It should only be used for a one-time setup. """

import os
import pymysql
import pandas as pd
#import argparse

#parser = argparse.ArgumentParser(description='Puts data from ICD10 codelists into a database called cura_db')
#parser.add_argument('folder_path', type=str, )
#parser.add_argument('--reset', )

try:
    cnx = pymysql.connect(
        host="localhost",
        user="root",
        password="",
        db="cura_db"
    )
    cnx.autocommit = True
    cur = cnx.cursor()
    #cur.execute("truncate table medcode; truncate table medlist; truncate table medgroup;")

    path_string = "Z:\\Big_Data\\Codes\\readcode"

    def check_entity_already_exists(path, father_id):
        query = f"select group_id from medgroup where group_name = {os.path.basename(path)} and father_id = {father_id}"
        cur.execute(query)
        result = cur.fetchall()
        if result is None:
            return False
        

    def files_to_db(path, father_id):
        #query = f"select group_name from medgroup where father_id={father_id}"
        #cur.execute(query)
        #existing_dirs = cur.fetchall()
        if os.path.isdir(path):
            query = f"insert into medgroup (group_name, father_id) values ('{os.path.basename(path)}', {father_id})"
            cur.execute(query)
            query = "select max(group_id) from medgroup"
            cur.execute(query)
            group_id = cur.fetchone()[0]
            for x in os.listdir(path):
                if x[0] != "_" and x[0] != ".":
                    files_to_db(os.path.join(path, x), group_id)
        else:
            query = f"insert into medlist (list_name, group_id) values ('{os.path.basename(path)}', {father_id})"
            cur.execute(query)
            query = "select max(list_id) from medlist"
            cur.execute(query)
            list_id = cur.fetchone()[0]
            table = pd.read_csv(path, '\t')
            for row in table.to_numpy():
                query = f"insert into medcode values ({list_id}, '{row[5]}', '" + row[6].replace('\'', '\\\'') + f"',  {row[0]})"
                cur.execute(query)
        
    # Need to check if any folders/files have been deleted from the source
    query = "select group_name " 
    

    for file_name in os.listdir(path_string):
        # Create the full file path for the function
        fp = os.path.join(path_string, file_name)
        # Ignore folders/files starting with _ or . for example .git
        if file_name[0] == "_" or file_name[0] == "." or os.path.isfile(fp):
            continue
        files_to_db(fp, 'null')


finally:
    cnx.close()