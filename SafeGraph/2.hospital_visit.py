#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 28 17:08:28 2021

@author: yeabinmoon
"""

import pandas as pd
from safegraph_py_functions import safegraph_py_functions as sgpy
import time

df = pd.read_pickle('/Users/yeabinmoon/Documents/JMP/data/SafeGraph/POI/selected_poi.pickle')
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