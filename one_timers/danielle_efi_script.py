""" This creates a list of aurum eFI files from a spreadsheet containing a list of categories of ailments with SnomedCTCodes
    which have a one-to-many relationship with codes in the aurum dictionary """

import pandas
import pymysql as sql
import re


codes_file = "C:/Users/jonesn/downloads/eFI SNOMED (verified).xlsx"

output_directory = "Z:/big_data/under_review/_danielle_efi"

report_location = output_directory
report_name = "danielle_efi_report.csv"


with open(report_location+'/'+report_name, 'w') as report:
    report.writelines('filename\tcount\n')

df = pandas.read_excel(codes_file)
try:
    cnx = sql.Connect(
        host= 'localhost',
        user= 'root',
        password= '',
        db= 'cprd_codes',
    )
    cur = cnx.cursor()

    filename = ''
    header = True
    report_data = []

    for row in df.to_numpy():
        if filename != re.sub(r'/| / | - | ', '_', row[0]).lower():
            if report_data != []:
                with open(report_location+'/'+report_name, 'a') as report:
                    report.writelines(report_data[0]+'\t'+str(report_data[1])+'\n')
            filename = re.sub(r'/| / | - | ', '_', row[0]).lower()
            header = True
            report_data = [filename, 0]
        
        query = f'select * from aurum_medcode where snomedctconceptid = \'{row[3]}\''
        cur.execute(query)
        codes_df = pandas.DataFrame(cur.fetchall())

        if len(codes_df.count()) == 0:
            continue

        report_data[1]+=codes_df.count()[0]
        
        
        if header:
            with open(output_directory+'/'+filename+'.txt', 'w', encoding='UTF-8') as new:
                new.writelines('\t'.join(list([i[0] for i in cur.description])) + '\n')
            header = False
        
        with open(output_directory+'/'+filename+'.txt', 'a', encoding='UTF-8') as new:
            for code in codes_df.to_numpy():
                new.writelines(str(code[0])+'\t'+'\t'.join(code[1:]) + '\n')
    
    with open(report_location+'/'+report_name, 'a') as report:
        report.writelines(report_data[0]+'\t'+str(report_data[1])+'\n')

finally:
    cnx.close()