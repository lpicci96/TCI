## Jasmine (Kexin) Shang and Luca Picci 
## 2021.07.19
import pandas as pd
import numpy as np
import time
from itertools import product

path = r"C:\Users\lpicc\OneDrive\Documents\Pardee work\data team\TCI\BACI_HS92_Y1995_V202102.csv"
data = pd.read_csv(path)
data= data.drop('q', axis = 1)
data = data.drop('t', axis = 1)

start_time = time.time()
#get export and import agg values
exp_df = data.groupby(['i', 'k'], as_index=False).agg('sum').drop('j', axis = 1).rename(columns={'v':'v_exp'})
imp_df = data.groupby(['j', 'k'], as_index=False).agg('sum').drop('i', axis = 1).rename(columns={'v':'v_imp'})

#get all the unique combination of i & k
dt = data[['i','k']]
uniques = [dt[i].unique().tolist() for i in dt.columns ]
exp = pd.DataFrame(product(*uniques), columns = dt.columns)
exp_df = exp.merge(exp_df, on = ['i','k'], how = 'outer')

#get all the unique combination of j & k
dt = data[['j','k']]
uniques = [dt[i].unique().tolist() for i in dt.columns ]
imp = pd.DataFrame(product(*uniques), columns = dt.columns)
imp_df = imp.merge(imp_df, on = ['j','k'], how = 'outer')

#Merge exp_df and imp_df
df_ij = exp_df.merge(imp_df, on=['k'], how = 'outer')
#fill nan
df_ij.fillna(0,inplace = True)
#remove i==j
df_ij = df_ij[df_ij['i'] != df_ij['j']]

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
