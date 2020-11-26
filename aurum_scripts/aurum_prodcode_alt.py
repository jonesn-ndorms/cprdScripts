""" Like the aurum_prodcode.py script, this creates aurum product codelists from the gold product codelists, 
    but this supplements direct dmd joins with indirect dmd joins through the dmd tables """

import os
import pymysql as sql
from pymysql.constants import CLIENT
import pandas as pd


READ_PATH = "Z:\\Big_Data\\Codes\\prodcode_gold\\"
WRITE_PATH = "Z:\\Big_Data\\Codes\\prodcode_aurum\\_test\\"

WRITE_REPORT = True
SORT_REPORT = False
REPORT_LOCATION = "C:\\users\\jonesn\\"

query1 = (
    "create temporary table uig "
    "(PRIMARY KEY temp_pk (prodcode)) "
    "select a.prodcode, b.* from gold_prodcode as a "
    "inner join aurum_prodcode as b on a.dmdcode = b.dmdid "
    "where a.dmdcode != ''; "
)
query2 = (
    "select a.prodcode, b.* from gold_prodcode as a "
    "inner join aurum_prodcode as b on a.dmdcode = b.dmdid "
    "where a.dmdcode != '' "
    "union "
    "select * from ( "
    "    select e.prodcode, h.* from gold_prodcode as e "
    "    inner join dmd.amps as f on e.dmdcode = f.APID "
    "    inner join dmd.vmps as g on f.VPID = g.VPID "
    "    inner join aurum_prodcode as h on g.VPID = h.dmdid "
    "    union "
    "    select l.prodcode, i.* from aurum_prodcode as i "
    "    inner join dmd.amps as j on i.dmdid = j.APID "
    "    inner join dmd.vmps as k on j.VPID = k.VPID "
    "    inner join gold_prodcode as l on k.VPID = l.dmdcode "
    ") as p where p.prodcode not in ( "
    "    select prodcode from uig "
    "); "
)
    #"drop table uig; "
#)

try:
    cnx = sql.connect(
        host="localhost",
        user="root",
        password="",
        db="cprd_codes"
    )

    cur = cnx.cursor()
    
    cur.execute(query1)
    cur.execute(query2)
    aurum_prodcode = pd.DataFrame(cur.fetchall())
    aurum_prodcode.columns = [desc[0] for desc in cur.description]
    aurum_prodcode = aurum_prodcode.astype('string')

    if WRITE_REPORT:
        report = open(REPORT_LOCATION + "aurum_prodcode_report.txt", 'w', encoding='Latin-1')
        report.write('Name\tConverted\tOriginal Count\n')
        report.close()

    for folder, dirs, files in os.walk(READ_PATH, topdown=True):
        dirs[:] = [d for d in dirs if d[:1] not in [".", "_"]]
        files[:] = [f for f in files if f[-4:] == ".txt" and f not in ("prodcode_20200601.txt", "prodcode_20201019.txt") ]
        if not os.path.exists(WRITE_PATH + folder.replace(READ_PATH, "")):
            os.makedirs(WRITE_PATH + folder.replace(READ_PATH, ""))
        for f in files:
            df = pd.read_csv(folder + "\\" + f, sep="\t", usecols=['prodcode',], dtype={'prodcode': 'string'})
            df = df.dropna()
            join = df.join(aurum_prodcode.set_index("prodcode"), "prodcode", "inner", "a", "b")
            join = join.drop(columns='prodcode')
            join = join.drop_duplicates()
            if join.ProdCodeId.count() != 0:
                join.to_csv(WRITE_PATH + folder.replace(READ_PATH, "") + "\\" + f, sep="\t", index=False,
                    columns = ['ProdCodeId', 'dmdid', 'TermfromEMIS', 'ProductName', 'Formulation', 'RouteOfAdministration', 'DrugSubstanceName', 'SubstanceStrength', 'BNFChapter', 'Release']
                )
            if WRITE_REPORT:
                gold_file_count = df.prodcode.count()
                join_count = join.ProdCodeId.count()
                report = open(REPORT_LOCATION + "aurum_prodcode_report.txt", 'a', encoding='Latin-1')
                report.write(f"{f}\t{str(join_count)}\t{str(int(gold_file_count))}\n")
                report.close()
    
    if WRITE_PATH and SORT_REPORT:
        report = open(REPORT_LOCATION + "aurum_prodcode_report.txt", 'r')
        report_array = report.readlines()
        report.close()
        report = open(REPORT_LOCATION + "aurum_prodcode_report.txt", 'w', encoding='Latin-1')
        report.write(report_array[0] + report_array[1:].sort().join(''))
        report.close()

finally:
    cur.close()
