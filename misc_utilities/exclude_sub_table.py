""" Remove all of the codes from 'SUPER_TABLE' that exist in the 'SUB_TABLE' """

import pandas as pd

SUB_TABLE = "C:\\Users\\jonesn\\Desktop\\hospital_admission_unplanned.txt"
SUPER_TABLE = "C:\\Users\\jonesn\\Desktop\\hospital_admission_unplanned_extra.txt"

df_super = pd.read_csv(SUPER_TABLE, sep="\t", dtype="object")
df_sub = pd.read_csv(SUB_TABLE, sep="\t", dtype="object", usecols=["MedCodeId"])

df_all = df_super.merge(df_sub, on=['MedCodeId'], how='left', indicator=True)
df_exc = df_all.where(df_all["_merge"] == "left_only").dropna(subset=["MedCodeId"])


df_exc.to_csv(SUPER_TABLE, sep="\t", 
    columns=["MedCodeId", "Term", "OriginalReadCode", "CleansedReadCode", "SnomedCTConceptId", "SnomedCTDescriptionId", "Release", "EmisCodeCategoryId"],
    index=False
)