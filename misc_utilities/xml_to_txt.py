""" Converts xml data to flat text files, initially written for dm+d data """

import os
from xml.etree import ElementTree as ET

xml_folder = "Z://big_data//codes//dmd//nhsbsa_dmd//vtm//"

output_extension = '.csv'

schemas = [f for f in os.listdir(xml_folder) if os.path.splitext(f)[1] == '.xsd']
files_to_process = [f for f in os.listdir(xml_folder) if os.path.splitext(f)[1] == '.xml']

for n in range(len(files_to_process)):

    tree = ET.parse(xml_folder + files_to_process[n])
    root = tree.getroot()

    # Schema for new files and column headers
    schema_root = ET.parse(xml_folder + schemas[n]).getroot()
    ns = schema_root.tag.replace('schema', '') # ns = namespace
    new_file_root = schema_root.find(ns+'element').find(ns+'complexType').find(ns+'sequence')

    if len(new_file_root.findall(ns+'element')) > 1: # some files have multiple data tables types inside and thus need to be treated differently
        for nf in range(len(new_file_root.findall(ns+'element'))):
            new_file_name = new_file_root.findall(ns+'element')[nf].get('name')
            table_type = new_file_root.findall(ns+'element')[nf].find(ns+'complexType').find(ns+'sequence').find(ns+'element').get('type')
            cols = [col.get('name') for col in list(filter(lambda x: x.get('name') == table_type, schema_root.findall(ns+'complexType')))[0].find(ns+'all')]
            new = open(xml_folder + files_to_process[n].replace('.xml', '_') + new_file_name + output_extension, 'w', encoding='Latin-1')
            new.writelines('\t'.join(cols) + '\n')
            data_root = root[nf]
            for child in data_root:
                row = []
                for col in cols:
                    data = child.find(col).text if child.find(col) is not None else ''
                    row.append(data)
                new.writelines('\t'.join(row) + '\n')
            new.close()
    else:
        col_list = [col.get('name') for col in schema_root.find(ns+'element').find(ns+'complexType').find(ns+'sequence').find(ns+'element').find(ns+'complexType').find(ns+'sequence')]
        with open(xml_folder + files_to_process[n].replace('.xml', output_extension), 'w', encoding='Latin-1') as new:
            new.writelines('\t'.join(col_list) + '\n')
            for child in root:
                row = []
                for col in col_list:
                    data = child.find(col).text if child.find(col) is not None else ''
                    row.append(data)
                new.writelines('\t'.join(row) + '\n')
