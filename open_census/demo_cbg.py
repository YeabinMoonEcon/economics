
import pandas as pd

def wavg(group, avg_name, weight_name):
    """ http://stackoverflow.com/questions/10951341/pandas-dataframe-aggregate-function-using-multiple-columns
    In rare instance, we may not have weights, so just return the mean. Customize this if your business case
    should return otherwise.
    """
    d = group[avg_name]
    w = group[weight_name]
    try:
        return (d * w).sum() / w.sum()
    except ZeroDivisionError:
        return d.mean()

"""
decription: question code
"""

df_help = pd.read_csv('/Volumes/LaCie/cg-data/safegraph_open_census_data/metadata/cbg_field_descriptions.csv')
df_help.loc[df_help.iloc[:,1].str.contains('eteran'),:]





"""
race info
"""

df = pd.read_csv('/Volumes/LaCie/cg-data/safegraph_open_census_data/data/cbg_b03.csv',
                    dtype = {'census_block_group':str},
                    usecols = ['census_block_group',
                               'B03002e1','B03002e12','B03002e3','B03002e4','B03002e6'])

df.rename(columns = {'census_block_group':'poi_cbg',
                     'B03002e1':'total', 'B03002e12':'hispanic',
                     'B03002e3':'white','B03002e4':'black',
                     'B03002e6':'asian'}, inplace = True)

df.loc[:,'white'] = df.loc[:,'white'] / df.loc[:,'total']
df.loc[:,'black'] = df.loc[:,'black'] / df.loc[:,'total']
df.loc[:,'asian'] = df.loc[:,'asian'] / df.loc[:,'total']
df.loc[:,'hispanic'] = df.loc[:,'hispanic'] / df.loc[:,'total']

df.drop(columns = {'total'}, inplace = True)
df.to_csv('/Users/yeabinmoon/Documents/JMP/data/open_census/race_cbg.csv')
