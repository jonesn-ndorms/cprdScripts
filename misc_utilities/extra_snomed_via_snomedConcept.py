import pandas as pd
import os
import pymysql as sql

source_directory = 'Z:\\Big_Data\\Codes\\snomed\\mental_health\\'
output_directory = source_directory
suffix = '_extra'

WRITE_REPORT = True
report_location = "C:\\users\\jonesn\\documents\\data_reports\\"
report_name = "mental_health_extra_report.txt"

exclusion_files = [report_name] # if there's files in the source directory that you want the script to ignore, add the full filename as an element of the list

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
        report = open(report_location + report_name, 'w', encoding='Latin-1')
        report.write('Name\tExtra code count\tCodes present in other files\n')
        report.close()

    for folder, dirs, files in os.walk(source_directory, topdown=True):
        dirs[:] = [d for d in dirs if d[:1] not in [".", "_"]]
        files[:] = [f for f in files if f[-4:] == ".txt" and f not in exclusion_files and f[-len(suffix)-4:-4] != suffix]
        output_folder = output_directory + folder.replace(source_directory, "") + "\\"
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        for f in files:
            df = pd.read_csv(folder + '\\' + f, sep="\t", usecols=['SnomedCTConceptId', 'MedCodeId'], dtype={'SnomedCTConceptId': 'string', 'MedCodeId': 'string'})
            df = df.dropna()
            join = df.join(aurum_medcode.set_index('SnomedCTConceptId'), 'SnomedCTConceptId', "inner", "a", "b")
            # Once the extra codes have been found, they need to be disti
            join = join.merge(df, left_on='MedCodeIdb', right_on='MedCodeId', how='left', indicator=True)
            join = join.where(join["_merge"] == "left_only").dropna(subset=["MedCodeIdb"])
            join = join.drop(['MedCodeIda', 'MedCodeId', 'SnomedCTConceptId_y', '_merge'], axis=1)
            join = join.drop_duplicates()
            join = join.rename(columns={'MedCodeIdb': 'MedCodeId', 'SnomedCTConceptId_x': 'SnomedCTConceptId'})
            if join.MedCodeId.count() != 0:
                join.to_csv(output_folder + f[:-4] + suffix + f[-4:], sep="\t", index=False,
                    columns = ['MedCodeId','Term','OriginalReadCode','CleansedReadCode','SnomedCTConceptId','SnomedCTDescriptionId','Release','EmisCodeCategoryId']
                )
            if WRITE_REPORT:
                join_count = join.MedCodeId.count()
                common = []
                for fil in files:
                    if fil != f:
                        rep = pd.read_csv(folder + '\\' + fil, sep='\t', usecols=['MedCodeId']).astype(str)
                        rep = rep.join(join.set_index('MedCodeId'), 'MedCodeId', 'inner', 'a', 'b')
                        if rep.MedCodeId.count() != 0:
                            common.append(fil + ':' + ','.join(list(rep.MedCodeId)))
                report = open(report_location + report_name, 'a', encoding='Latin-1')
                report.write(f"{f[:-4] + suffix + f[-4:]}\t{str(int(join_count))}\t" + ';'.join(common) + "\n")
                report.close()
finally:   
    cnx.close()
