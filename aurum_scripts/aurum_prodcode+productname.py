""" Like the aurum_prodcode.py script, this creates aurum product codelists from the gold product codelists, 
    but this uses the product term as well as the dmd code for the join between datasets """

import os
import pymysql as sql
import pandas as pd


READ_PATH = "Z:\\Big_Data\\Codes\\prodcode_gold\\"
WRITE_PATH = "Z:\\Big_Data\\Codes\\prodcode_aurum\\_productname_test\\"

WRITE_REPORT = True
SORT_REPORT = False
REPORT_LOCATION = "C:\\users\\jonesn\\"

try:
    cnx = sql.connect(
        host="localhost",
        user="root",
        password="",
        db="cprd_codes"
    )

    cur = cnx.cursor()
    query = ("select b.prodcode, a.* from aurum_prodcode as a "
             "inner join gold_prodcode b "
             "on a.dmdid = b.dmdcode or a.productname = b.productname "
             "where a.dmdid != '';")
    cur.execute(query)
    aurum_prodcode = pd.DataFrame(cur.fetchall())
    aurum_prodcode.columns = [desc[0] for desc in cur.description]
    aurum_prodcode = aurum_prodcode.astype('string')

    if WRITE_REPORT:
        report = open(REPORT_LOCATION + "aurum_prodcode+productname_report.txt", 'w', encoding='Latin-1')
        report.write('Name\tConverted\tNot converted\n')
        report.close()

    for folder, dirs, files in os.walk(READ_PATH, topdown=True):
        dirs[:] = [d for d in dirs if d[:1] not in [".", "_"]]
        files[:] = [f for f in files if f[-4:] == ".txt" and f != "prodcode_20200601.txt"]
        if not os.path.exists(WRITE_PATH + folder.replace(READ_PATH, "")):
            os.makedirs(WRITE_PATH + folder.replace(READ_PATH, ""))
        for f in files:
            df = pd.read_csv(folder + "\\" + f, sep="\t", usecols=['prodcode',], dtype={'prodcode': 'string'})
            df = df.dropna()
            join = df.join(aurum_prodcode.set_index("prodcode"), "prodcode", "inner", "a", "b")
            join = join.drop_duplicates()
            if join.ProdCodeId.count() != 0:
                join.to_csv(WRITE_PATH + folder.replace(READ_PATH, "") + "\\" + f, sep="\t", index=False,
                    columns = ['ProdCodeId', 'dmdid', 'TermfromEMIS', 'ProductName', 'Formulation', 'RouteOfAdministration', 'DrugSubstanceName', 'SubstanceStrength', 'BNFChapter', 'Release']
                )
            if WRITE_REPORT:
                gold_file_count = df.dmdid.count()
                join_count = join.dmdid.count()
                report = open(REPORT_LOCATION + "aurum_prodcode+productname_report.txt", 'a', encoding='Latin-1')
                report.write(f"{f}\t{str(join_count)}\t{str(int(gold_file_count - join_count))}\n")
                report.close()
    if WRITE_PATH and SORT_REPORT:
        report = open(REPORT_LOCATION + "aurum_prodcode+productname_report.txt", 'r')
        report_array = report.readlines()
        report.close()
        report = open(REPORT_LOCATION + "aurum_prodcode+productname_report.txt", 'w', encoding='Latin-1')
        report.write(report_array[0] + report_array[1:].sort().join(''))
        report.close()

finally:
    cur.close()
