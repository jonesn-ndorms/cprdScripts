import os
import pandas as pd
import pymysql as sql


settings = {
    'source_directory': 'D:/icd10_codes_to_map/',
    'source_delimiter': ',',
    'source_icd_column_name': 'ICD10 CODE',
    'output_directory': 'D:/icd10_codes_to_map/alg_results/read',
    'process_sub_directories': False,
    'valid_file_extensions': ['.csv', '.txt'],              # only files with this extension will be read
    'exclusion_files': [],
    'output_extension': '.txt',
    'report': True,                                         # tells the algorithm to create a report
    'report_location': 'D:/icd10_codes_to_map/alg_results',
    'report_name': 'icd10_extraction_report.txt',
    'result_coding': 'read',                                # 'read' or 'snomed'
    'write_link_files': True,                               # write files that contain linkage details for checking/analysis
    'icd_source': ['ICD10', 'ICD10CM'] # Determines which vocabulary to use for the codes. The list order signals preference. 
}                                      # Make sure what you write corresponds exactly to the cdm vocabulary name

settings['exclusion_files'].append(settings['report_name']) # exclude processing the report in case the report_location within source_directory

def sql_connect(db):
    cnx = sql.connect(
        host='localhost',
        user='root',
        password='',
        db=db
    )
    return cnx

def create_staging_tables(source, col_name):
    """ create and populate a staging database """
    try:
        cnx_s = sql_connect('cdm_vocabulary_53')
        cnx_s.autocommit(True)
        cur_s = cnx_s.cursor()
        
        staging_tables = []

        if source[-5:] == '.xlsx':
            pass
        else:
            filename = os.path.splitext(os.path.basename(source))[0]
            staging_name = 'staging_' + filename.lower()
            cur_s.execute(f'drop table if exists `{staging_name}`')
            cur_s.execute(f'create table {staging_name} (icd10 varchar(10) not null)')
            df = pd.read_csv(source, sep=settings['source_delimiter'])
            row_index = df.columns.get_loc(col_name)
            for row in df.to_numpy():
                cur_s.execute(f"insert into {staging_name} (icd10) values ('{row[row_index]}');")
            staging_tables.append(staging_name)
        # cnx_s.commit()
    finally:
        cnx_s.close()
    return staging_tables

def drop_staging_tables(staging_tables):
    try:
        cnx_s = sql_connect('cdm_vocabulary_53')
        cur_s = cnx_s.cursor()
        for staging_name in staging_tables:
            cur_s.execute(f"drop table if exists `{staging_name}`")
    finally:
        cnx_s.close()


def create_query(result_coding, icd_source, staging_name):
    alias = 'e' if result_coding == 'read' else 'c'
    if len(icd_source) == 1:
        query = (
            "select "
            "    group_concat(distinct i.icd10 separator '; ') as source_icd10, "
            "    group_concat(distinct a.concept_code separator '; ') as ICD10, "
            "    group_concat(distinct a.concept_name separator '; ') as ICD10_term, "
            f"   {alias}.concept_id, "
            f"   max({alias}.concept_name) as concept_name, "
            f"   max({alias}.concept_code) as concept_code, "
            f"   max({alias}.domain_id) as domain_id, "
            f"   max({alias}.vocabulary_id) as vocabulary_id, "
            f"   max({alias}.concept_class_id) as concept_class_id, "
            f"   max({alias}.valid_start_date) as valid_start_date, "
            f"   max({alias}.valid_end_date) as valid_end_date, "
            f"   max({alias}.invalid_reason) as invalid_reason "
            f"from {staging_name} as i "
            "inner join concept as a "
            "on "
            "    a.concept_code = if(icd10 like '%.%' and lower(right(icd10, 1)) != 'x', icd10, null)  "
            "    or left(a.concept_code, length(if(icd10 not like '%.%' or lower(right(icd10, 1)) = 'x', replace(replace(icd10, 'X', ''), 'x', ''), null))) =  "
            "        if(icd10 not like '%.%' or lower(right(icd10, 1)) = 'x', replace(replace(icd10, 'X', ''), 'x', ''), null) "
            "inner join concept_relationship as b on a.concept_id = b.concept_id_1 "
            "inner join concept as c on b.concept_id_2 = c.concept_id "
        )
        if result_coding == 'snomed':
            query += (
                f"where a.vocabulary_id = '{icd_source[0]}' and c.vocabulary_id = 'SNOMED' "
                "group by c.concept_code; "
            )
        elif result_coding == 'read':
            query += (
                "inner join concept_relationship as d on c.concept_id = d.concept_id_1 "
                "inner join concept as e on d.concept_id_2 = e.concept_id "
                f"where a.vocabulary_id = '{icd_source[0]}' and c.vocabulary_id = 'SNOMED' and e.vocabulary_id = 'Read' "
                "group by e.concept_code; "
            )
            
    elif len(icd_source) == 2:
        query = (
            "select "
            "    group_concat(distinct i.icd10 separator '; ') as source_icd10, "
            "    group_concat(distinct i.concept_code separator '; ') as ICD10, "
            "    group_concat(distinct i.concept_name separator '; ') as ICD10_term, "
            f"   {alias}.concept_id, "
            f"   max({alias}.concept_name) as concept_name, "
            f"   max({alias}.concept_code) as concept_code, "
            f"   max({alias}.domain_id) as domain_id, "
            f"   max({alias}.vocabulary_id) as vocabulary_id, "
            f"   max({alias}.concept_class_id) as concept_class_id, "
            f"   max({alias}.valid_start_date) as valid_start_date, "
            f"   max({alias}.valid_end_date) as valid_end_date, "
            f"   max({alias}.invalid_reason) as invalid_reason, "
            f"   group_concat(distinct i.{icd_source[1]} separator '; ') as {icd_source[1]} "
            " from ( "
            f"   select *, 0 as {icd_source[1]} from staffa1 "
            "    union "
            f"   select ii.icd10, a.concept_code, a.concept_id, a.concept_name, 1 as {icd_source[1]} from {staging_name} as ii "
            "    inner join concept as a on "
            "        a.concept_code = if(icd10 like '%.%' and lower(right(icd10, 1)) != 'x', icd10, null)  "
            "        or left(a.concept_code, length(if(icd10 not like '%.%' or lower(right(icd10, 1)) = 'x', replace(replace(icd10, 'X', ''), 'x', ''), null))) = "
            "            if(icd10 not like '%.%' or lower(right(icd10, 1)) = 'x', replace(replace(icd10, 'X', ''), 'x', ''), null) "
            f"   where a.concept_code not in (select concept_code from staffa2) and a.vocabulary_id = '{icd_source[1]}' "
            ") i "
            "inner join concept_relationship as b on i.concept_id = b.concept_id_1 "
            "inner join concept as c on b.concept_id_2 = c.concept_id "
        )
        if result_coding == 'snomed':
            query += (
                "where c.vocabulary_id = 'SNOMED' "
                "group by c.concept_code; "
            )
        elif result_coding == 'read':
            query += (
                "inner join concept_relationship as d on c.concept_id = d.concept_id_1 "
                "inner join concept as e on d.concept_id_2 = e.concept_id "
                "where c.vocabulary_id = 'SNOMED' and e.vocabulary_id = 'Read' "
                "group by e.concept_code; "
            )
    return query


try:
    cnx1 = sql_connect('cprd_codes')
    cnx2 = sql_connect('cdm_vocabulary_53')

    cur = cnx1.cursor()
    if settings['result_coding'] == 'snomed':
        cur.execute(f'select * from aurum_medcode')
    elif settings['result_coding'] == 'read':
        cur.execute(f'select * from gold_medcode')
    cprd = pd.DataFrame(cur.fetchall())
    cprd.columns = [desc[0] for desc in cur.description]
    cprd = cprd.astype(str)
    cur.close()
    
    cnx2.autocommit(True)
    cur = cnx2.cursor()
    
    for folder, dirs, files in os.walk(settings['source_directory']):
        if settings['process_sub_directories']:
            dirs[:] = [d for d in dirs if d[:1] not in [".", "_"]]
        else:
             dirs[:] = []
        files[:] = [f for f in files if f[-4:] in settings['valid_file_extensions'] and f not in settings['exclusion_files']]
        write_folder = settings['output_directory'] + folder.replace(settings['source_directory'], "") + "/"
        if not os.path.exists(write_folder):
            os.makedirs(write_folder)
        for f in files:
            staging_tables = create_staging_tables(folder+"/"+f, settings['source_icd_column_name'])
            for staging_name in staging_tables:
                if len(settings['icd_source']) > 1:
                    cur.execute("drop table if exists `staffa1`;")
                    cur.execute("drop table if exists `staffa2`;")
                    cur.execute(
                        "create temporary table staffa1 "
                        "(primary key temp_pk (icd10, concept_code)) "
                        "select distinct i.icd10, a.concept_code, a.concept_id, a.concept_name "
                        f"from {staging_name} as i "
                        "inner join concept as a on "
                        "   a.concept_code= if(icd10 like '%.%' and lower(right(icd10, 1)) != 'x', icd10, null) "
                        "   or left(a.concept_code, length(if(icd10 not like '%.%' or lower(right(icd10, 1)) = 'x', replace(replace(icd10, 'X', ''), 'x', ''), null))) = "
                        "       if(icd10 not like '%.%' or lower(right(icd10, 1)) = 'x', replace(replace(icd10, 'X', ''), 'x', ''), null) "
                        "where a.vocabulary_id = 'ICD10';"
                    )
                    cur.execute(
                        "create temporary table staffa2 "
                        "(primary key temp_pk (icd10, concept_code)) "
                        "select * from staffa1;"
                    )
                cur.execute(create_query(settings['result_coding'], settings['icd_source'], staging_name))
                df = pd.DataFrame(cur.fetchall())
                df.columns = [desc[0] for desc in cur.description]
                df = df.astype(str)
                if settings['result_coding'] == 'snomed':
                    df = df.rename(columns={'concept_code': 'SnomedCTConceptId'})
                elif settings['result_coding'] == 'read':
                    df = df.rename(columns={'concept_code': 'readcode'})
                df = df.dropna()

                if len(settings['icd_source']) > 1:
                    cur.execute("drop table if exists `staffa1`;")
                    cur.execute("drop table if exists `staffa2`;")
                
                filename = staging_name.replace('staging_', '')
                if settings['write_link_files']:
                    link_folder = write_folder + 'link/'
                    if not os.path.isdir(link_folder):
                        os.mkdir(link_folder)
                    df.to_csv(link_folder + filename + '_link' + settings['output_extension'], sep='\t', index=False)
                
                if settings['result_coding'] == 'snomed':
                    join = df.join(cprd.set_index('SnomedCTConceptId'), 'SnomedCTConceptId', 'inner')
                    join = join.drop_duplicates()
                    if join.MedCodeId.count()>0:
                        join.to_csv(write_folder + filename + settings['output_extension'], sep='\t', index=False,
                            columns=['MedCodeId', 'Term', 'OriginalReadCode', 'CleansedReadCode', 'SnomedCTConceptId', 'SnomedCTDescriptionId', 'Release', 'EmisCodeCategoryId']
                        )
                elif settings['result_coding'] == 'read':
                    join = df.join(cprd.set_index('readcode'), 'readcode', 'inner')
                    join = join.drop_duplicates()
                    if join.medcode.count()>0:
                        join.to_csv(write_folder + filename + settings['output_extension'], sep='\t', index=False,
                            columns=['medcode', 'readcode', 'clinicalevents', 'immunisationevents', 'referralevents', 'testevents', 'readterm', 'databasebuild']
                        )
            drop_staging_tables(staging_tables)
finally:
    cnx1.close()
    cnx2.close()