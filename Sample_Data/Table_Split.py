import pandas as pd
import os
import csv
from sqlalchemy import create_engine
import datetime
import dateutil
ct = datetime.datetime.now()
print("current time:-", ct)

input_file = r"Sample_Data\Working11-30-23.xlsx"

orignal_df = pd.read_excel(input_file, sheet_name='WPForms')
#print(orignal_df.head())
category_df = pd.read_excel(input_file, sheet_name="Attribute Category List")
#print(category_df.head())

main_header = category_df[category_df['Table'] == 'Main']['Attribute'].tolist()
partner_header = category_df[category_df['Table'] == 'Project Partners']['Attribute'].tolist()
storage_loc_header = category_df[category_df['Table'] == 'Storage Location']['Attribute'].tolist()
capture_source_header = category_df[category_df['Table'] == 'Capture Source']['Attribute'].tolist()
capture_source_2_header =  category_df[category_df['Table'] == 'Capture Source 2']['Attribute'].tolist()
class_6_wells_header = category_df[category_df['Table'] == 'Class VI Wells']['Attribute'].tolist()
reservoir_header = category_df[category_df['Table'] == 'Reservoir']['Attribute'].tolist()
monitoring_header= category_df[category_df['Table'] == 'Monitoring Wells']['Attribute'].tolist()
pipeline_header = category_df[category_df['Table'] == 'Pipeline Segments']['Attribute'].tolist()


df_main = orignal_df[orignal_df.columns.intersection(main_header)]
df_partner = orignal_df[orignal_df.columns.intersection(partner_header)]
df_storage_loc = orignal_df[orignal_df.columns.intersection(storage_loc_header)]
df_capture_source = orignal_df[orignal_df.columns.intersection(capture_source_header)]
df_capture_source_2 = orignal_df[orignal_df.columns.intersection(capture_source_2_header)]
df_class_6_wells = orignal_df[orignal_df.columns.intersection(class_6_wells_header)]
df_reservoir = orignal_df[orignal_df.columns.intersection(reservoir_header)]
df_monitoring = orignal_df[orignal_df.columns.intersection(monitoring_header)]
df_pipeline = orignal_df[orignal_df.columns.intersection(pipeline_header)]

starttime = datetime.datetime.now()
print(starttime)


'''print(len(df_main))
df_main.to_sql('main', engine)
endtime = datetime.datetime.now()
print("Time", endtime-starttime) 

df_main.to_sql('main', engine)
df_partner.to_sql('partner', engine)
df_storage_loc.to_sql('storage_loc', engine)
df_capture_source.to_sql('capture_source', engine)
df_capture_source_2.to_sql('capture_source_2', engine)
df_class_6_wells.to_sql('class_6_wells', engine)
df_reservoir.to_sql('reservoir', engine)
df_monitoring.to_sql('monitoring', engine)
df_pipeline.to_sql('pipeline', engine)
endtime = datetime.datetime.now()
print("Time", endtime-starttime) '''

df_main.to_csv('main.csv')
