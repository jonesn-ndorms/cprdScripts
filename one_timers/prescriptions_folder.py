""" Creates ATC folders and files from a spreadsheet and text file specifying folder structure and file contents respectively. """

import os
import pandas
import re


source_file = "C:/Users/jonesn/downloads/prescriptions_structure_Nathan.xlsx"

# output_directory = "Z:/PDE_Group/05_ucb_pass/07_data_management/codes/extraction_1/prescriptions"
output_directory = "Z:big_data/under_review"

merged_result_file = "C:/Users/jonesn/downloads/merged_result.txt"


dir_df = pandas.read_excel(source_file)
dir_df = dir_df.dropna(how='all')

mer_df = pandas.read_csv(merged_result_file, ',', quotechar='\"', encoding='windows-1252')

report_folder = output_directory
report_name = 'ATC_report.txt'
with open(report_folder + '/' + report_name, 'w', encoding='Latin-1') as report:
    report.writelines('filename\tcode count\n')

first_level = None
overall_path = ''

for row in dir_df.to_numpy():
    if pandas.notna(row[0]):
        # remove some characters completely
        first_level = row[0].strip().replace('\n', '')
        # replace some charachters with _
        first_level = re.sub(r'-| - | |, ', '_', first_level)
        # lowercase
        first_level = first_level.lower()
        if not os.path.isdir(output_directory + '/' + first_level):
            os.makedirs(output_directory + '/' + first_level)
    if pandas.notna(row[2]):
        # remove some characters completely
        second_level = (row[2].strip() + '_' + row[1].strip()).replace('\n', '').replace('->', '')
        # replace some charachters with _
        second_level = re.sub(r'-| - | |, ', '_', second_level)
        # lowercase
        second_level = second_level.lower()
        # ensure that filepaths aren't too long (leaving 56 bytes for file names...)
        overall_path = (output_directory + '/' + first_level + '/' + second_level)[:200]
        if not os.path.isdir(overall_path):
            os.makedirs(overall_path)
    if pandas.notna(row[3]):
        data_subset = mer_df.loc[mer_df['ATC'] == row[3].strip()]
        filename = (row[3].strip() + '_' + row[4].strip()).replace('\n', '')
        filename = re.sub(r'-[^->]| - | |, ', '_', filename).lower()
        filename = filename[:51] + '.txt'
        if data_subset.prodcode.count() != 0: 
            data_subset.to_csv(overall_path + '/' + filename, sep='\t', index=False,
                columns=["prodcode","dmdcode","therapyevents","gemscriptcode","productname","drugsubstancename","substancestrength","formulation","routeofadministration","bnfcode","bnfheader","databasebuild"])
        with open(report_folder + '/' + report_name, 'a', encoding='Latin-1') as report:
            report.writelines(f'{filename}\t{str(data_subset.prodcode.count())}\n')