
import pandas as pd
from safegraph_py_functions import safegraph_py_functions as sgpy


temp_df = pd.read_csv('/Volumes/LaCie/cg-data/Pattern_1/2020/02/patterns-part2.csv.gz',
                      usecols = ['safegraph_place_id','location_name','city','region','raw_visitor_counts','visitor_home_cbgs'])

df1 = temp_df.loc[temp_df.region == 'TX',:]
df1 = df1.loc[df1.city == 'Houston',:]
temp = df1.loc[df1.location_name.str.contains('Turkey Leg'),:]
