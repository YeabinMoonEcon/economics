"""
Created on Jan 14 2021

@author: yeabinmoon

Using monthly visit patterns prior to pandemic (- 2020.02) label which race dominates it

0. Process monthly patterns to see what race distribution of POI

outputs: monthly files in /Users/yeabinmoon/Documents/JMP/data/SafeGraph/race_visitor/monthly

"""

import pandas as pd
import time
from safegraph_py_functions import safegraph_py_functions as sgpy


years = ['2018', '2019']
months = pd.date_range(start='2018-01-01', end='2018-12-31',freq= 'M')
months = list(months.strftime('%m'))
list_files = ['patterns-part1.csv.gz','patterns-part2.csv.gz',
              'patterns-part3.csv.gz','patterns-part4.csv.gz']

demo = pd.read_csv('/Users/yeabinmoon/Dropbox (UH-ECON)/Research/JMP/data/open_census/race_cbg.csv',
                   index_col = 0, dtype = {'poi_cbg':str})



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
                              compression = 'gzip', dtype = {'poi_cbg':str})
            temp = pd.concat([temp, temp_df], axis = 0, ignore_index=True)
        temp = sgpy.unpack_json_and_merge_fast(temp,json_column = 'visitor_home_cbgs',chunk_n = 1000)
        temp.drop(columns = {'visitor_home_cbgs'},inplace = True)
        temp.rename(columns = {'visitor_home_cbgs_key':'poi_cbg'}, inplace = True)
        temp = temp.merge(demo, how = 'left', on = 'poi_cbg')
        temp.loc[:,'white'] = temp.loc[:,'visitor_home_cbgs_value'] * temp.loc[:,'white']
        temp.loc[:,'black'] = temp.loc[:,'visitor_home_cbgs_value'] * temp.loc[:,'black']
        temp.loc[:,'asian'] = temp.loc[:,'visitor_home_cbgs_value'] * temp.loc[:,'asian']
        temp.loc[:,'hispanic'] = temp.loc[:,'visitor_home_cbgs_value'] * temp.loc[:,'hispanic']
        temp = temp.groupby('safegraph_place_id')['visitor_home_cbgs_value','white','black','asian','hispanic'].sum()

        temp.loc[:,'visitor_home_cbgs_value'] = temp.loc[:,'visitor_home_cbgs_value'].apply(pd.to_numeric, downcast = 'integer')
        temp.iloc[:,1:] = temp.iloc[:,1:].apply(pd.to_numeric, downcast = 'float')
        temp.to_pickle('/Users/yeabinmoon/Documents/JMP/data/SafeGraph/race_visitor/monthly/'+year+'-'+month+'.pickle.gz',
                       compression = 'gzip')
        print("Done", year+'-'+month)
        print("%f seconds" % (time.time() - start_time_month))


years = ['2020']
months = ['01','02']


for year in years:


    for month in months:
        start_time_month = time.time()
        temp = pd.DataFrame()
        for files in list_files:
            temp_df = pd.read_csv('/Volumes/LaCie/cg-data/Pattern/'+year+'/'+month+'/'+files,
                              usecols = ['safegraph_place_id','visitor_home_cbgs'],
                              compression = 'gzip', dtype = {'poi_cbg':str})
            temp = pd.concat([temp, temp_df], axis = 0, ignore_index=True)
        temp = sgpy.unpack_json_and_merge_fast(temp,json_column = 'visitor_home_cbgs',chunk_n = 1000)
        temp.drop(columns = {'visitor_home_cbgs'},inplace = True)
        temp.rename(columns = {'visitor_home_cbgs_key':'poi_cbg'}, inplace = True)
        temp = temp.merge(demo, how = 'left', on = 'poi_cbg')
        temp.loc[:,'white'] = temp.loc[:,'visitor_home_cbgs_value'] * temp.loc[:,'white']
        temp.loc[:,'black'] = temp.loc[:,'visitor_home_cbgs_value'] * temp.loc[:,'black']
        temp.loc[:,'asian'] = temp.loc[:,'visitor_home_cbgs_value'] * temp.loc[:,'asian']
        temp.loc[:,'hispanic'] = temp.loc[:,'visitor_home_cbgs_value'] * temp.loc[:,'hispanic']
        temp = temp.groupby('safegraph_place_id')['visitor_home_cbgs_value','white','black','asian','hispanic'].sum()

        temp.loc[:,'visitor_home_cbgs_value'] = temp.loc[:,'visitor_home_cbgs_value'].apply(pd.to_numeric, downcast = 'integer')
        temp.iloc[:,1:] = temp.iloc[:,1:].apply(pd.to_numeric, downcast = 'float')
        temp.to_pickle('/Users/yeabinmoon/Documents/JMP/data/SafeGraph/race_visitor/monthly/'+year+'-'+month+'.pickle.gz',
                       compression = 'gzip')
        print("Done", year+'-'+month)
        print("%f seconds" % (time.time() - start_time_month))
