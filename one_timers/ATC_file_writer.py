import pymysql as sql
import pandas


ATC_file = "C:/Users/jonesn/downloads/drug_result2.txt"

output_folder = "Z:/big_data/under_review"

df = pandas.read_csv(ATC_file, sep=',', quotechar='\"')

for row in df.to_numpy():
    