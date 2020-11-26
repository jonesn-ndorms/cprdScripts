""" Populates cura_db with codes and hierarchical structure of the icd10 folder
    WARNING: this is not yet set up for updating the database. It should only be used for a one-time setup. """

import os
import pymysql
import pandas as pd
#import argparse

#parser = argparse.ArgumentParser(description='Puts data from ICD10 codelists into a database called cura_db')
#parser.add_argument(type=str, )

try:
    cnx = pymysql.connect(
        host="localhost",
        user="root",
        password="",
        db="cura_db"
    )
    cnx.autocommit = True
    cur = cnx.cursor()
    # Shouldn't truncate the tables in production as this would put the list_id's out of kilter with list_id's in curator
    cur.execute("truncate table icd10code;")
    cur.execute("truncate table icd10list;")
    cur.execute("truncate table icd10group;")

    #TODO: make the algorithm update code file structure (will involve comparing old and new) and renewing lists (can be a truncated)
    path_string = "Z:\\Big_Data\\Codes\\icd10"

    def check_files_were_removed(path):
        query = "select group_id "

    def files_to_db(path, father_id):
        query = ""
        if os.path.isdir(path):
            query = f"insert into icd10group (group_name, father_id) values ('{os.path.basename(path)}', {father_id})"
            cur.execute(query)
            query = "select max(group_id) from icd10group"
            cur.execute(query)
            group_id = cur.fetchone()[0]
            for x in os.listdir(path):
                if x[0] != "_" and x[0] != ".":
                    files_to_db(os.path.join(path, x), group_id)
        else:
            query = f"insert into icd10list (list_name, group_id) values ('{os.path.basename(path)}', {father_id})"
            cur.execute(query)
            query = "select max(list_id) from icd10list"
            cur.execute(query)
            list_id = cur.fetchone()[0]
            table = pd.read_csv(path, '\t')
            for row in table.to_numpy():
                query = f"insert into icd10code values ({list_id}, '{row[0]}', '" + row[1].replace('\'', '\\\'') + "')"
                cur.execute(query)
        
    
    for file_name in os.listdir(path_string):
        fp = os.path.join(path_string, file_name)
        if file_name[0] == "_" or file_name[0] == "." or os.path.isfile(fp):
            continue
        files_to_db(fp, 'null')


finally:
    cnx.close()