#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar  1 16:28:33 2021

@author: yeabinmoon
"""

import pandas as pd
from safegraph_py_functions import safegraph_py_functions as sgpy
import time


df = pd.read_pickle('/Users/yeabinmoon/Documents/JMP/data/SafeGraph/POI/temp/temp2.pickle.gz')
poi = pd.read_pickle('/Users/yeabinmoon/Documents/JMP/data/SafeGraph/POI/POI.pickle')
hospital = poi.loc[poi.top_category == 'General Medical and Surgical Hospitals',:]
hospital = hospital.merge(df, how = 'left', on = 'safegraph_place_id')


temp = (hospital.region == 'AL') |(hospital.region == 'FL') | (hospital.region == 'GA') | (hospital.region == 'KY') | (hospital.region == 'MS') | (hospital.region == 'NC') |        (hospital.region == 'SC') | (hospital.region == 'TN')

df = hospital.loc[temp,:]
df = df.loc[df.iloc[:,20:20+12].isnull().sum(axis = 1) < 1,:]

#df.iloc[:,20].quantile([.1,.25,.5,.75,.9])
#df.iloc[:,20:20+12].mean(axis = 1).quantile([.05,.1,.25,.5,.75,.9])

df = df.loc[df.iloc[:,20:20+12].mean(axis = 1) > 20,:]
df.iloc[:,20:].sum().plot()

poi_list = df.iloc[:,:8]
poi_list.groupby('region').count()
poi_list.to_pickle('/Users/yeabinmoon/Documents/JMP/data/SafeGraph/POI/poi_southeast.pickle')



df = pd.read_pickle('/Users/yeabinmoon/Documents/JMP/data/SafeGraph/POI/poi_southeast.pickle')
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
                                  usecols = ['safegraph_place_id','visitor_home_cbgs'],
                                  compression = 'gzip')
            temp = pd.concat([temp, temp_df], axis = 0, ignore_index= True)
        temp_ = df.merge(temp, how = 'left', on = 'safegraph_place_id')
    
        temp_.dropna(inplace = True)
        temp_ = sgpy.unpack_json_and_merge_fast(temp_,json_column = 'visitor_home_cbgs',chunk_n = 1000)
    
        temp_ = temp_.groupby('visitor_home_cbgs_key')['visitor_home_cbgs_value'].sum()
        temp_ = temp_.reset_index()
        temp_.rename(columns = {'visitor_home_cbgs_value':year+'-'+month}, inplace = True)
        temp_.to_pickle('/Users/yeabinmoon/Documents/JMP/data/SafeGraph/POI/temp/monthly/'+year+'-'+month+'.pickle.gz',
                        compression = 'gzip')

        print("Done",month,year)
        print("%f seconds" % (time.time() - start_time_month))

year = '2020'
for month in ['01','02','03','04']:
    start_time_month = time.time()
    temp = pd.DataFrame()
    for files in list_files:
        temp_df = pd.read_csv('/Volumes/LaCie/cg-data/Pattern_1/'+year+'/'+month+'/'+files,
                              usecols = ['safegraph_place_id','visitor_home_cbgs'],
                              compression = 'gzip')
        temp = pd.concat([temp, temp_df], axis = 0, ignore_index=True)  
    temp_ = df.merge(temp, how = 'left', on = 'safegraph_place_id')
    temp_.dropna(inplace = True)
    temp_ = sgpy.unpack_json_and_merge_fast(temp_,json_column = 'visitor_home_cbgs',chunk_n = 1000)
    
    temp_ = temp_.groupby('visitor_home_cbgs_key')['visitor_home_cbgs_value'].sum()
    temp_ = temp_.reset_index()
    temp_.rename(columns = {'visitor_home_cbgs_value':year+'-'+month},inplace = True)
    temp_.to_pickle('/Users/yeabinmoon/Documents/JMP/data/SafeGraph/POI/temp/monthly/'+year+'-'+month+'.pickle.gz',
                    compression = 'gzip')
    
    print("Done",month,year)
    print("%f seconds" % (time.time() - start_time_month))


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
                              usecols = ['safegraph_place_id','visitor_home_cbgs'],
                              compression = 'gzip')
        temp = pd.concat([temp, temp_df], axis = 0, ignore_index=True)  
        
    temp_ = df.merge(temp, how = 'left', on = 'safegraph_place_id')
    temp_.dropna(inplace = True)
    temp_ = sgpy.unpack_json_and_merge_fast(temp_,json_column = 'visitor_home_cbgs',chunk_n = 1000)
    
    temp_ = temp_.groupby('visitor_home_cbgs_key')['visitor_home_cbgs_value'].sum()
    temp_ = temp_.reset_index()
    temp_.rename(columns = {'visitor_home_cbgs_value':month}, inplace = True)
    temp_.to_pickle('/Users/yeabinmoon/Documents/JMP/data/SafeGraph/POI/temp/monthly/'+month+'.pickle.gz',
                    compression = 'gzip')

    i += 1
    print("Done", month)
    print("%f seconds" % (time.time() - start_time_month))
    


months = pd.date_range(start='2018-01-01', end='2021-01-31',freq= 'M')
months = list(months.strftime('%Y-%m'))

df = pd.read_pickle('/Users/yeabinmoon/Documents/JMP/data/SafeGraph/POI/temp/monthly/2018-01.pickle.gz')
for month in months[1:]:
    temp = pd.read_pickle('/Users/yeabinmoon/Documents/JMP/data/SafeGraph/POI/temp/monthly/'+month+'.pickle.gz')
    df = df.merge(temp, how = 'outer', on = 'visitor_home_cbgs_key')

df.to_pickle('/Users/yeabinmoon/Documents/JMP/data/SafeGraph/POI/tract.pickle.gz',compression = 'gzip')

df = pd.read_pickle('/Users/yeabinmoon/Documents/JMP/data/SafeGraph/POI/tract.pickle.gz',compression = 'gzip')
df = df.loc[df.visitor_home_cbgs_key.str[:2].astype(int) < 60,:]
df.loc[:,'state'] = df.visitor_home_cbgs_key.str[:2]
#temp = df.groupby('state')['2019-01'].count()
temp = (df.state == '01') |(df.state == '12') | (df.state == '13') | (df.state == '21') | (df.state == '28') | (df.state == '37') | (df.state == '45') | (df.state == '47')
df = df.loc[temp,:]
df.fillna(0,inplace = True)

df.iloc[:,:-1].to_pickle('/Users/yeabinmoon/Documents/JMP/data/SafeGraph/POI/df.pickle')

df.iloc[:,1:-1].sum().plot()
df.iloc[:,12:-1].sum().plot(title = 'Inpatient Hospital visiting patterns in South East')
