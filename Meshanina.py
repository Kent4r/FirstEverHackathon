#%% IMPORTS
import pandas as pd
import numpy as np
import re
from datetime import datetime
# %%

df = pd.read_csv('ds_dirty_fin_202410041147.csv')
# %%

def split_fio(full_fio):
    if re.search(r'[0-9]', full_fio):
        return None

    fio_list = full_fio.title().split(maxsplit=2)
    try:
        surname = fio_list[0]
    except IndexError:
        surname = None

    try:
        name = fio_list[1]
    except IndexError:
        name = None

    try:
        patronymic = fio_list[2]
    except IndexError:
        patronymic = None
    res = [surname, name, patronymic]
    return res

def clean_part(part):
    if isinstance(part, str):
        part = re.sub(r'/.*', '', part)
        if '.' in part or ',' in part:
            part = part.replace('.', '').replace(',', '')
    return part if part and len(part) > 1 else np.nan

def process_fio(fio):
    parts = split_fio(fio)
    if parts is None:
        return [np.nan, np.nan, np.nan]
    return [clean_part(part) for part in parts]

# Заменяю все некоректные даты ДР, людей младше 14 и старше 100 на NaN
def change_date(x):
    try:
        date = (datetime.now() - datetime.strptime(x, "%Y-%m-%d")).days
        if 14*365 <= date <= 100*365:
            return x
        else:
            return np.nan
    except:
        return np.nan

# Заменяю ссылки на NaN
df = df[~df['client_cityzen'].astype(str).str.contains('http', case=False)]

df['client_bday'] = df['client_bday'].apply(change_date)
# %%

# Обрабатываем ФИО
fio_parts = df['client_fio_full'].value_counts().index.tolist()
fio_parts = [process_fio(fio) for fio in fio_parts]

surname_list = [part[0] for part in fio_parts]
name_list = [part[1] for part in fio_parts]
patronymic_list = [part[2] for part in fio_parts]

full_fio_list = [f"{surname} {name} {patronymic}".upper() for surname, name, patronymic in zip(surname_list, name_list, patronymic_list)]
# %%

df_res = df.copy()

fio_dict = dict(zip(df['client_fio_full'].value_counts().index.tolist(), full_fio_list))
names_dict = dict(zip(df['client_first_name'].value_counts().index.tolist(), name_list))
surnames_dict = dict(zip(df['client_last_name'].value_counts().index.tolist(), surname_list))
patronymics_dict = dict(zip(df['client_middle_name'].value_counts().index.tolist(), patronymic_list))
# %%

# Применяем замены
df_res['client_fio_full'] = df_res['client_fio_full'].apply(lambda x: fio_dict.get(x, x))
df_res['client_first_name'] = df_res['client_first_name'].apply(lambda x: names_dict.get(x, x)).apply(lambda x: x.title() if pd.notna(x) else x)
df_res['client_last_name'] = df_res['client_last_name'].apply(lambda x: surnames_dict.get(x, x)).apply(lambda x: x.title() if pd.notna(x) else x)
df_res['client_middle_name'] = df_res['client_middle_name'].apply(lambda x: patronymics_dict.get(x, x)).apply(lambda x: x.title() if pd.notna(x) else x)
# %%

# Слияние строк клиента в одну в датаврейм golden_df
df_res = df_res.sort_values(by='update_date', ascending=False)

golden_df = df_res.groupby('client_fio_full').first().reset_index()

for col in df_res.columns:
    if col != 'client_fio_full':
        golden_df[col] = golden_df[col].combine_first(df.groupby('client_fio_full')[col].last())
# %%

golden_df.shape
# %%

golden_df.head()
# %%
