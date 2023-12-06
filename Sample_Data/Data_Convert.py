import csv
import pandas as pd
import os
from sqlalchemy import create_engine
import datetime
import dateutil
import psycopg2
ct = datetime.datetime.now()
print("current time:-", ct)

engine = create_engine('postgresql://postgres:l1v1ngD4t4b4s3!@10.67.10.38:5005/carbon_safe')

                       
input_table = r"Sample_Data\Working_Partner_Table.xlsx"
df = pd.read_excel(input_table, sheet_name=0)

starttime = datetime.datetime.now()
print(starttime)


print(len(df))
df.to_sql('Working_Partner_Table2', engine)
endtime = datetime.datetime.now()
print("Time", endtime-starttime)               