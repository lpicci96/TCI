## Luca Picci 7.18.2021
import pandas as pd
import numpy as np
import time
path = r"C:\Users\lpicc\OneDrive\Documents\Pardee work\data team\TCI\BACI_HS92_Y1995_V202102.csv"
data = pd.read_csv(path)
data= data.drop('q', axis = 1)
data = data.drop('t', axis = 1)

start_time = time.time()
#get export and import agg values
exp_df = data.groupby(['i', 'k'], as_index=False).agg('sum').drop('j', axis = 1).rename(columns={'v':'v_exp'})
imp_df = data.groupby(['j', 'k'], as_index=False).agg('sum').drop('i', axis = 1).rename(columns={'v':'v_imp'})

#make sure all codes are accounted for
codes = list(set(imp_df.k.unique())|set(exp_df.k.unique()))
for code in codes:
    for exporter in list(exp_df.i.unique()):
        if code not in list(exp_df.loc[exp_df.i == exporter, 'k']):
            exp_df = exp_df.append({'i':exporter, 'k':code, 'v_exp':0}, ignore_index=True)
        else:
            pass
    for importer in list(imp_df.j.unique()):
        if code not in list(imp_df.loc[imp_df.j == importer, 'k']):
            imp_df = imp_df.append({'j':importer, 'k':code, 'v_imp':0}, ignore_index=True)
        else:
            pass


#create new df
df_ij = pd.merge(exp_df, imp_df, on = ['k'], how='outer')

#calc total exports and imports
imp_tot = imp_df.groupby('j').agg('sum').drop('k', axis = 1).rename(columns={'v_imp':'imp_tot'})
exp_tot = exp_df.groupby('i').agg('sum').drop('k', axis=1).rename(columns={'v_exp':'exp_tot'})



#merging totals
df_ij = pd.merge(df_ij, imp_tot, how='left', on='j')
df_ij = pd.merge(df_ij, exp_tot, how = 'left', on = 'i')

#calculating shares
df_ij['code_share'] = abs((df_ij.v_exp/df_ij.exp_tot)-(df_ij.v_imp/df_ij.imp_tot))
df_ij = df_ij.drop(['v_exp', 'v_imp', 'imp_tot', 'exp_tot', 'k'], axis =1)

#aggregate shares
df_ij = df_ij.groupby(['i', 'j'], as_index=False).agg('sum')

#calculating tci
df_ij['tci'] = (1-(df_ij.code_share/2))*100
df_ij = df_ij.drop('code_share', axis = 1)

#results
time_taken = round(time.time() - start_time, 4)
print(f'complete in {time_taken} s')
print(df_ij[(df_ij.i == 156)&(df_ij.j == 842)])
