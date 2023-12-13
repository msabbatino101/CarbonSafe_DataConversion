import pandas as pd
import os
import csv
from sqlalchemy import create_engine
import datetime
import dateutil
ct = datetime.datetime.now()
print("current time:-", ct)

#this is the connectioni strings to connect to a database server
#engine = create_engine('postgresql:')


print("connected")
input_file = r"C:\Projects\GitHub\CarbonSafe_DataConversion\Sample_Data\Working11-30-23.xlsx"

orignal_df = pd.read_excel(input_file, sheet_name='WPForms')
#print(orignal_df.head())
category_df = pd.read_excel(input_file, sheet_name="Attribute Category List")
#print(category_df.head())

#create headers for each table
main_header = category_df[category_df['Table'] == 'Main']['Attribute'].tolist()
partner_header = category_df[category_df['Table'] == 'Project Partners']['Attribute'].tolist()
storage_loc_header = category_df[category_df['Table'] == 'Storage Location']['Attribute'].tolist()
capture_source_header = category_df[category_df['Table'] == 'Capture Source']['Attribute'].tolist()
capture_source_2_header =  category_df[category_df['Table'] == 'Capture Source 2']['Attribute'].tolist()
class_6_wells_header = category_df[category_df['Table'] == 'Class VI Wells']['Attribute'].tolist()
reservoir_header = category_df[category_df['Table'] == 'Reservoir']['Attribute'].tolist()
monitoring_header= category_df[category_df['Table'] == 'Monitoring Wells']['Attribute'].tolist()
pipeline_header = category_df[category_df['Table'] == 'Pipeline Segments']['Attribute'].tolist()

#extract data for each table into separate dataframe
df_main = orignal_df[orignal_df.columns.intersection(main_header)]
df_partner = orignal_df[orignal_df.columns.intersection(partner_header)]
df_storage_loc = orignal_df[orignal_df.columns.intersection(storage_loc_header)]
df_capture_source = orignal_df[orignal_df.columns.intersection(capture_source_header)]
df_capture_source_2 = orignal_df[orignal_df.columns.intersection(capture_source_2_header)]
df_class_6_wells = orignal_df[orignal_df.columns.intersection(class_6_wells_header)]
df_reservoir = orignal_df[orignal_df.columns.intersection(reservoir_header)]
df_monitoring = orignal_df[orignal_df.columns.intersection(monitoring_header)]
df_pipeline = orignal_df[orignal_df.columns.intersection(pipeline_header)]

#fix pipelines
df_pipeline.reset_index(inplace=True)
df_pipeline_new1 = df_pipeline.iloc[:,:28]
df_pipeline_new2 = df_pipeline.iloc[:,28:55]
df_pipeline_new2.reset_index(inplace=True)
df_pipeline_new3 = df_pipeline.iloc[:,55:82]
df_pipeline_new3.reset_index(inplace=True)
df_pipeline_new4 = df_pipeline.iloc[:,82:]
df_pipeline_new4.reset_index(inplace=True)
df_pipeline_new4['field_905'] = ''
df_pipeline_new2.columns = df_pipeline_new1.columns
df_pipeline_new3.columns = df_pipeline_new1.columns
df_pipeline_new4.columns = df_pipeline_new1.columns
df_pipeline_new = pd.concat([df_pipeline_new1, df_pipeline_new2, df_pipeline_new3, df_pipeline_new4], axis=0)

#fix monitoring
df_monitoring.reset_index(inplace=True)
df_monitoring_new = pd.DataFrame(columns = ['index','well_id','latitude', 'longitude', 'purpose'])
for i, row in enumerate(df_monitoring.iterrows()):
    if df_monitoring.loc[i, 'well_id_31'] == df_monitoring.loc[i, 'well_id_31']:
        new_row1 = {'index':row[1][0],'well_id':row[1][1], 'latitude':row[1][8], 'longitude':row[1][15],'purpose':row[1][22],}
        #print(new_row1)
        #df_partner_new.append(new_row1, ignore_index=True)
        df_monitoring_new1 = pd.DataFrame(new_row1, index=[i])
        df_monitoring_new = pd.concat([df_monitoring_new, df_monitoring_new1], axis=0)
        new_row2 = {'index':row[1][0],'well_id':row[1][2], 'latitude':row[1][9], 'longitude':row[1][16],'purpose':row[1][23],}
        df_monitoring_new1 = pd.DataFrame(new_row2, index=[i])
        df_monitoring_new = pd.concat([df_monitoring_new, df_monitoring_new1], axis=0)
        new_row3 = {'index':row[1][0],'well_id':row[1][3], 'latitude':row[1][10], 'latitude':row[1][17],'purpose':row[1][24],}
        df_monitoring_new1 = pd.DataFrame(new_row3, index=[i])
        df_monitoring_new = pd.concat([df_monitoring_new, df_monitoring_new1], axis=0)
        new_row4 = {'index':row[1][0],'well_id':row[1][4], 'latitude':row[1][11], 'longitude':row[1][18],'purpose':row[1][25],}
        df_monitoring_new1 = pd.DataFrame(new_row4, index=[i])
        df_monitoring_new = pd.concat([df_monitoring_new, df_monitoring_new1], axis=0)
        new_row5 = {'index':row[1][0],'well_id':row[1][5], 'latitude':row[1][12], 'longitude':row[1][19],'purpose':row[1][26],}
        df_monitoring_new1 = pd.DataFrame(new_row5, index=[i])
        df_monitoring_new = pd.concat([df_monitoring_new, df_monitoring_new1], axis=0)
        new_row6 = {'index':row[1][0],'well_id':row[1][6], 'latitude':row[1][13], 'longitude':row[1][20],'purpose':row[1][27],}
        df_monitoring_new1 = pd.DataFrame(new_row6, index=[i])
        df_monitoring_new = pd.concat([df_monitoring_new, df_monitoring_new1], axis=0)     
        new_row7 = {'index':row[1][0],'well_id':row[1][7], 'latitude':row[1][14], 'longitude':row[1][21],'purpose':row[1][28],}
        df_monitoring_new1 = pd.DataFrame(new_row7, index=[i])
        df_monitoring_new = pd.concat([df_monitoring_new, df_monitoring_new1], axis=0)
        
#fix class 6 wells
df_class_6_wells.reset_index(inplace=True)
df_class_6_wells_new = pd.DataFrame(columns = ['index','well_id', 'permit_no_where_available','latitude', 'longitude'])
for i, row in enumerate(df_class_6_wells.iterrows()):
    if df_class_6_wells.loc[i, 'well_id'] == df_class_6_wells.loc[i, 'well_id']:
        new_row1 = {'index':row[1][0],'well_id':row[1][1], 'permit_no_where_available':row[1][7], 'latitude':row[1][13],'longitude':row[1][19],}
        #print(new_row1)
        #df_partner_new.append(new_row1, ignore_index=True)
        df_class_6_wells_new1 = pd.DataFrame(new_row1, index=[i])
        df_class_6_wells_new = pd.concat([df_class_6_wells_new, df_class_6_wells_new1], axis=0)
        new_row2 = {'index':row[1][0],'well_id':row[1][2], 'permit_no_where_available':row[1][8], 'latitude':row[1][14],'longitude':row[1][20],}
        df_class_6_wells_new1 = pd.DataFrame(new_row2, index=[i])
        df_class_6_wells_new = pd.concat([df_class_6_wells_new, df_class_6_wells_new1], axis=0)
        new_row3 = {'index':row[1][0],'well_id':row[1][3], 'permit_no_where_available':row[1][9], 'latitude':row[1][15],'longitude':row[1][21],}
        df_class_6_wells_new1 = pd.DataFrame(new_row3, index=[i])
        df_class_6_wells_new = pd.concat([df_class_6_wells_new, df_class_6_wells_new1], axis=0)
        new_row4 = {'index':row[1][0],'well_id':row[1][4], 'permit_no_where_available':row[1][10], 'latitude':row[1][16],'longitude':row[1][22],}
        df_class_6_wells_new1 = pd.DataFrame(new_row4, index=[i])
        df_class_6_wells_new = pd.concat([df_class_6_wells_new, df_class_6_wells_new1], axis=0)
        new_row5 = {'index':row[1][0],'well_id':row[1][5], 'permit_no_where_available':row[1][11], 'latitude':row[1][17],'longitude':row[1][23],}
        df_class_6_wells_new1 = pd.DataFrame(new_row5, index=[i])
        df_class_6_wells_new = pd.concat([df_class_6_wells_new, df_class_6_wells_new1], axis=0)
        new_row6 = {'index':row[1][0],'well_id':row[1][6], 'permit_no_where_available':row[1][12], 'latitude':row[1][18],'longitude':row[1][24],}
        df_class_6_wells_new1 = pd.DataFrame(new_row6, index=[i])
        df_class_6_wells_new = pd.concat([df_class_6_wells_new, df_class_6_wells_new1], axis=0)   
    
#fix storage loc
df_storage_loc.reset_index(inplace=True)
df_storage_loc_new = pd.DataFrame(columns = ['index','storage_facility_state', 'what_counties_parishes_are_the_storage_facilities'])
for i, row in enumerate(df_storage_loc.iterrows()):
    if df_storage_loc.loc[i, 'storage_facility_state'] == df_storage_loc.loc[i, 'storage_facility_state']:
        new_row1 = {'index':row[1][0],'storage_facility_state':row[1][1], 'what_counties_parishes_are_the_storage_facilities':row[1][3]}
        df_storage_loc_new1 = pd.DataFrame(new_row1, index=[i])
        df_storage_loc_new = pd.concat([df_storage_loc_new, df_storage_loc_new1], axis=0)
        new_row2 = {'index':row[1][0],'storage_facility_state':row[1][2], 'what_counties_parishes_are_the_storage_facilities':row[1][4]}
        df_storage_loc_new1 = pd.DataFrame(new_row2, index=[i])
        df_storage_loc_new = pd.concat([df_storage_loc_new, df_storage_loc_new1], axis=0)

#fix partner
df_partner.reset_index(inplace=True)
df_partner_new = pd.DataFrame(columns = ['index','project_partners', 'organization_type'])
for i, row in enumerate(df_partner.iterrows()):
    if df_partner.loc[i, 'project_partners'] == df_partner.loc[i, 'project_partners']:
        new_row1 = {'index':row[1][0],'project_partners':row[1][1], 'organization_type':row[1][6]}
        #print(new_row1)
        #df_partner_new.append(new_row1, ignore_index=True)
        df_partner_new1 = pd.DataFrame(new_row1, index=[i])
        df_partner_new = pd.concat([df_partner_new, df_partner_new1], axis=0)
        new_row2 = {'index':row[1][0],'project_partners':row[1][2], 'organization_type':row[1][7]}
        df_partner_new1 = pd.DataFrame(new_row2, index=[i])
        df_partner_new = pd.concat([df_partner_new, df_partner_new1], axis=0)
        new_row3 = {'index':row[1][0],'project_partners':row[1][3], 'organization_type':row[1][8]}
        df_partner_new1 = pd.DataFrame(new_row3, index=[i])
        df_partner_new = pd.concat([df_partner_new, df_partner_new1], axis=0)
        new_row4 = {'index':row[1][0],'project_partners':row[1][4], 'organization_type':row[1][9]}
        df_partner_new1 = pd.DataFrame(new_row4, index=[i])
        df_partner_new = pd.concat([df_partner_new, df_partner_new1], axis=0)
        new_row5 = {'index':row[1][0],'project_partners':row[1][5], 'organization_type':row[1][10]}
        df_partner_new1 = pd.DataFrame(new_row5, index=[i])
        df_partner_new = pd.concat([df_partner_new, df_partner_new1], axis=0)


#This section is to export the data inton csv files
df_main.to_csv('main.csv')
df_partner_new.to_csv('partner.csv')
df_storage_loc_new.to_csv('storage_loc.csv')
df_capture_source.to_csv('capture_source.csv')
df_capture_source_2.to_csv('capture_source_2.csv')
df_class_6_wells_new.to_csv('class_6_wells.csv')
df_reservoir.to_csv('reservoir.csv')
df_monitoring_new.to_csv('monitoring.csv')
df_pipeline_new.to_csv('pipeline.csv')

starttime = datetime.datetime.now()
print(starttime)


#this section is to load the data onto a database
'''print(len(df_main))
df_main.to_sql('main', engine)
endtime = datetime.datetime.now()
print("Time", endtime-starttime) 

df_main.to_sql('main', engine)
df_partner_new.to_sql('partner', engine)
df_storage_loc_new.to_sql('storage_loc', engine)
df_capture_source.to_sql('capture_source', engine)
df_capture_source_2.to_sql('capture_source_2', engine)
df_class_6_wells_new.to_sql('class_6_wells', engine)
df_reservoir.to_sql('reservoir', engine)
df_monitoring_new.to_sql('monitoring', engine)
df_pipeline_new.to_sql('pipeline', engine)
endtime = datetime.datetime.now()
print("Time", endtime-starttime) '''

df_main.to_csv('main.csv')