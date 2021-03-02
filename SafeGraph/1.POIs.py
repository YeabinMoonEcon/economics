#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Feb 27 23:57:54 2021

@author: yeabinmoon
"""

import pandas as pd
import time

file_lists = ['core_poi-part1.csv.gz', 'core_poi-part2.csv.gz',
              'core_poi-part3.csv.gz', 'core_poi-part4.csv.gz',
              'core_poi-part5.csv.gz']

months = ['03','04','05','06','07','08','09','10']


df = pd.DataFrame()
for month in months:
    temp_df = pd.DataFrame()
    for files in file_lists:
        temp = pd.read_csv('/Volumes/LaCie/cg-data/CorePlaces/2020/'+month+'/'+files,
                           dtype = {'naics_code':str,'postal_code':str})
        temp = temp.loc[temp.naics_code.str[:3] == '622',:]        
        temp_df = pd.concat([temp_df, temp], axis = 0, ignore_index=True)
    df = pd.concat([df, temp_df], axis = 0, ignore_index=True)
    df = df.loc[~df.duplicated(subset = ['safegraph_place_id']),:]
    
temp_df = pd.DataFrame()   
for files in file_lists:
    temp = pd.read_csv('/Volumes/LaCie/cg-data/CorePlaces/core_poi/2020/11/06/11/'+files,
                       dtype = {'naics_code':str,'postal_code':str})
    temp = temp.loc[temp.naics_code.str[:3] == '622',:]        
    temp_df = pd.concat([temp_df, temp], axis = 0, ignore_index=True)

df = pd.concat([df, temp_df], axis = 0, ignore_index=True)
df = df.loc[~df.duplicated(subset = ['safegraph_place_id']),:]

temp_df = pd.DataFrame()   
for files in file_lists:
    temp = pd.read_csv('/Volumes/LaCie/cg-data/CorePlaces/core_poi/2020/11/06/12/'+files,
                       dtype = {'naics_code':str,'postal_code':str})
    temp = temp.loc[temp.naics_code.str[:3] == '622',:]        
    temp_df = pd.concat([temp_df, temp], axis = 0, ignore_index=True)

df = pd.concat([df, temp_df], axis = 0, ignore_index=True)
df = df.loc[~df.duplicated(subset = ['safegraph_place_id']),:]

temp_df = pd.DataFrame()   
for files in file_lists:
    temp = pd.read_csv('/Volumes/LaCie/cg-data/CorePlaces/core_poi/2020/12/04/04/'+files,
                       dtype = {'naics_code':str,'postal_code':str})
    temp = temp.loc[temp.naics_code.str[:3] == '622',:]        
    temp_df = pd.concat([temp_df, temp], axis = 0, ignore_index=True)

df = pd.concat([df, temp_df], axis = 0, ignore_index=True)
df = df.loc[~df.duplicated(subset = ['safegraph_place_id']),:]

temp_df = pd.DataFrame()   
for files in file_lists:
    temp = pd.read_csv('/Volumes/LaCie/cg-data/CorePlaces/core_poi/2021/01/06/11/'+files,
                       dtype = {'naics_code':str,'postal_code':str})
    temp = temp.loc[temp.naics_code.str[:3] == '622',:]        
    temp_df = pd.concat([temp_df, temp], axis = 0, ignore_index=True)

df = pd.concat([df, temp_df], axis = 0, ignore_index=True)
df = df.loc[~df.duplicated(subset = ['safegraph_place_id']),:]

temp_df = pd.DataFrame()   
for files in file_lists:
    temp = pd.read_csv('/Volumes/LaCie/cg-data/CorePlaces/core_poi/2021/02/04/06/'+files,
                       dtype = {'naics_code':str,'postal_code':str})
    temp = temp.loc[temp.naics_code.str[:3] == '622',:]        
    temp_df = pd.concat([temp_df, temp], axis = 0, ignore_index=True)

df = pd.concat([df, temp_df], axis = 0, ignore_index=True)
df = df.loc[~df.duplicated(subset = ['safegraph_place_id']),:]

temp_df = pd.DataFrame()   
for files in file_lists:
    temp = pd.read_csv('/Volumes/LaCie/cg-data/CorePlaces/core_poi/2021/02/08/08/'+files,
                       dtype = {'naics_code':str,'postal_code':str})
    temp = temp.loc[temp.naics_code.str[:3] == '622',:]        
    temp_df = pd.concat([temp_df, temp], axis = 0, ignore_index=True)

df = pd.concat([df, temp_df], axis = 0, ignore_index=True)
df = df.loc[~df.duplicated(subset = ['safegraph_place_id']),:]

df.to_pickle('/Users/yeabinmoon/Documents/JMP/data/SafeGraph/POI/hospital.pickle')

df = pd.read_pickle('/Users/yeabinmoon/Documents/JMP/data/SafeGraph/POI/hospital.pickle') 

cols = ['safegraph_place_id','location_name','top_category','naics_code',
        'latitude','longitude','city','region']
df = df.loc[:,cols]

df = df.loc[(df.region != "AS") & (df.region != "GU") & (df.region != "PR") & (df.region != "VI"),:]

df.to_pickle('/Users/yeabinmoon/Documents/JMP/data/SafeGraph/POI/POI.pickle')


# Filter Hospital using monthly visitng data

df = pd.read_pickle('/Users/yeabinmoon/Documents/JMP/data/SafeGraph/POI/POI.pickle')
df = df.loc[:,['safegraph_place_id']]

years = ['2018','2019']
months = pd.date_range(start='2018-01-01', end='2018-12-31',freq= 'M')
months = list(months.strftime('%m'))
list_files = ['patterns-part1.csv.gz','patterns-part2.csv.gz',
              'patterns-part3.csv.gz','patterns-part4.csv.gz']

year = years[0]
month = months[0]
files = list_files[0]

for year in years:
    for month in months:
        start_time_month = time.time()
        temp = pd.DataFrame()
        for files in list_files:
            temp_df = pd.read_csv('/Volumes/LaCie/cg-data/Pattern_1/'+year+'/'+month+'/'+files,
                                  usecols = ['safegraph_place_id','raw_visit_counts'],
                                  compression = 'gzip')
            temp = pd.concat([temp, temp_df], axis = 0, ignore_index=True)       
        df = df.merge(temp, how = 'left', on = 'safegraph_place_id')
        df.rename(columns = {'raw_visit_counts':year+'-'+month}, inplace = True)
        print("Done", year+'-'+month)
        print("%f seconds" % (time.time() - start_time_month))

df.to_pickle('/Users/yeabinmoon/Documents/JMP/data/SafeGraph/POI/temp/temp1.pickle.gz',
             compression = 'gzip')

year = '2020'
for month in ['01','02','03','04']:
    start_time_month = time.time()
    temp = pd.DataFrame()
    for files in list_files:
        temp_df = pd.read_csv('/Volumes/LaCie/cg-data/Pattern_1/'+year+'/'+month+'/'+files,
                              usecols = ['safegraph_place_id','raw_visit_counts'],
                              compression = 'gzip')
        temp = pd.concat([temp, temp_df], axis = 0, ignore_index=True)       
    df = df.merge(temp, how = 'left', on = 'safegraph_place_id')
    df.rename(columns = {'raw_visit_counts':year+'-'+month}, inplace = True)
    print("Done", year+'-'+month)
    print("%f seconds" % (time.time() - start_time_month))
df.to_pickle('/Users/yeabinmoon/Documents/JMP/data/SafeGraph/POI/temp/temp1.pickle.gz',
             compression = 'gzip')
    
directories = ['/Volumes/LaCie/cg-data/Pattern_2/patterns/2020/06/05/06/',
               '/Volumes/LaCie/cg-data/Pattern_2/patterns/2020/07/06/06/',
               '/Volumes/LaCie/cg-data/Pattern_2/patterns/2020/08/05/09/',
               '/Volumes/LaCie/cg-data/Pattern_2/patterns/2020/09/04/09/',
               '/Volumes/LaCie/cg-data/Pattern_2/patterns/2020/10/07/02/',
               '/Volumes/LaCie/cg-data/Pattern_2/patterns/2020/11/06/11/',
               '/Volumes/LaCie/cg-data/Pattern_3/patterns/2020/12/04/04/',
               '/Volumes/LaCie/cg-data/Pattern_3/patterns/2021/01/06/10/',
               '/Volumes/LaCie/cg-data/Pattern_3/patterns/2021/02/04/06/']


months = ['2020-05','2020-06','2020-07','2020-08','2020-09','2020-10','2020-11','2020-12',
          '2021-01']
i = 0

for directory in directories:
    start_time_month = time.time()
    temp = pd.DataFrame()
    month = months[i]
    for files in list_files:
        temp_df = pd.read_csv(directory+files,
                              usecols = ['safegraph_place_id','raw_visit_counts'],
                              compression = 'gzip')
        temp = pd.concat([temp, temp_df], axis = 0, ignore_index=True)  
    df = df.merge(temp, how = 'left', on = 'safegraph_place_id')
    df.rename(columns = {'raw_visit_counts':month}, inplace = True)
    i += 1
    print("Done", month)
    print("%f seconds" % (time.time() - start_time_month))
    
df.to_pickle('/Users/yeabinmoon/Documents/JMP/data/SafeGraph/POI/temp/temp2.pickle.gz',
             compression = 'gzip')

    
df = pd.read_pickle('/Users/yeabinmoon/Documents/JMP/data/SafeGraph/POI/temp/temp2.pickle.gz')
poi = pd.read_pickle('/Users/yeabinmoon/Documents/JMP/data/SafeGraph/POI/POI.pickle')
hospital = poi.loc[poi.top_category == 'General Medical and Surgical Hospitals',:]
hospital = hospital.merge(df, how = 'left', on = 'safegraph_place_id')

hospital.loc[:,'2018'] = hospital.iloc[:,20-12:20].sum(axis = 1)
hospital.loc[:,'2019'] = hospital.iloc[:,20:20+12].sum(axis = 1)
hospital.loc[:,'2020'] = hospital.iloc[:,20+12:20+12+12].sum(axis = 1)
hospital = hospital.loc[hospital.loc[:,'2018']>0,:]
hospital = hospital.loc[hospital.loc[:,'2019']>0,:]

hospital = hospital.loc[hospital.loc[:,'2019'] > 100,:]
hospital = hospital.loc[hospital.loc[:,'2018'] > 100,:]

hospital = hospital.loc[hospital.loc[:,'2019'] < hospital.loc[:,'2019'].quantile(.995),:]
hospital.to_pickle('/Users/yeabinmoon/Documents/JMP/data/SafeGraph/POI/selected_poi.pickle')
