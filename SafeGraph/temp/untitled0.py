#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 16 22:44:26 2021

@author: yeabinmoon
"""

import pandas as pd

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
                              usecols = ['safegraph_place_id','poi_cbg'],
                              compression = 'gzip', dtype = {'poi_cbg':str})
            temp = pd.concat([temp, temp_df], axis = 0, ignore_index=True)
        temp.set_index('safegraph_place_id',inplace = True)

temp_df = pd.read_pickle('/Users/yeabinmoon/Documents/JMP/data/SafeGraph/race_visitor/monthly/2018-01.pickle.gz')
temp_df.loc[:,'sh_white'] = temp_df.white / temp_df.visitor_home_cbgs_value
temp_df.loc[:,'sh_black'] = temp_df.black / temp_df.visitor_home_cbgs_value
temp_df.loc[:,'sh_asian'] = temp_df.asian / temp_df.visitor_home_cbgs_value
temp_df.loc[:,'sh_hispanic'] = temp_df.hispanic / temp_df.visitor_home_cbgs_value
temp_ = temp_df.loc[:,['sh_white','sh_black','sh_asian','sh_hispanic']]

temp_ = pd.concat([temp_,temp],axis=1)
temp_.reset_index(inplace = True)
temp_ = temp_.merge(demo, how = 'left', on = 'poi_cbg')
temp_.loc[:,'id_white'] = temp_.sh_white > temp_.white
temp_.loc[:,'id_black'] = temp_.sh_black > temp_.black
temp_.loc[:,'id_asian'] = temp_.sh_asian > temp_.asian
temp_.loc[:,'id_hispanic'] = temp_.sh_hispanic > temp_.hispanic

c = temp_.iloc[:1000]

temp_df.columns

demo


a = temp.iloc[:100]
