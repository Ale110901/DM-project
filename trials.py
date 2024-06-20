import pandas as pd
import numpy as np

df1 = pd.read_csv('./Datasets/owid-energy-data.csv', sep=',')
df2 = pd.read_csv('./Datasets/yearly_full_release_long_format.csv', sep=',')

df1 = df1['country'].unique()
df2 = df2['Area'].unique()

for c in df2:
    if not(np.where(df1 == c)):
        print(c)