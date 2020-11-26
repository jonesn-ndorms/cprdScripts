""" A somewhat redundant bit of code for checking the differences between readcode and snomed codelists.
    The need for checking the differences is usually met with the reports produced by the aurum_medcode script. """

import os
import pandas as pd
from collections import Counter


READ_PATH = "Z:\\Big_Data\\Codes\\readcode\\parkinson\\"
SNOMED_PATH = "Z:\\Big_Data\\Codes\\snomed\\parkinson\\"

FILES_TO_CHECK = [f for f in os.walk(READ_PATH) if f[2] != [] and f[2][0][-4:] == ".txt"]

with open(SNOMED_PATH + "check.txt", 'w', encoding="Latin-1") as check:
    for f in FILES_TO_CHECK:
        df1 = pd.read_csv(f[0] + "\\" + f[2][0], sep="\t", usecols=["readcode"])
        df2 = pd.read_csv(f[0].replace("\\readcode\\", "\\snomed\\") + "\\" + f[2][0], sep="\t", usecols=["CleansedReadCode"])
        err = False
        writeLine = f[2][0]
        setdiff = ''
        count = 0
        readcodes = ""
        if df1['readcode'].count() != df2['CleansedReadCode'].count():
            err = True
            diff1 = Counter(df1['readcode'].tolist()) - Counter(df2['CleansedReadCode'].tolist())
            diff2 = Counter(df2['CleansedReadCode'].tolist()) - Counter(df1['readcode'].tolist())
            setdiff = "readcode: " + str(list(diff1.elements())) + " snomed: " + str(list(diff2.elements()))
        else:
            for i in range(len(df1['readcode'].tolist())):
                if df1['readcode'].tolist()[i] != df2['CleansedReadCode'].tolist()[i]:
                    err = True
                    count += 1
                    readcodes += df1['readcode'].tolist()[i] + ", "
        if err:
            if count == 0:
                writeLine += " set-difference(" + setdiff + ")\n"
            else:
                writeLine += " count: " + count + "readcodes: (" + reacodes + ")\n"
            check.writelines(writeLine)