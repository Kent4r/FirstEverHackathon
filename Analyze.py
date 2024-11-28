#%% IMPORTS
import pandas as pd
import numpy as np
import re
from datetime import datetime
import time
# /stopwatch for code/
start_time = time.time()
# %%
df = pd.read_csv('ds_dirty_fin_202410041147.csv')
df.head(10)
# %%
df.shape
# %%
df.info()
# %%
df.isnull().sum()
# %%
df.columns.tolist
# %%
df['client_first_name'].value_counts()
# %%
df['client_child_cnt'].value_counts()
# %%
df['source_cd'].value_counts()
# %%
df['update_date'].value_counts().index.tolist()
# %%
df[df['client_cityzen']=='Д']

# %%
