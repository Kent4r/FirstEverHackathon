#%% IMPORTS
import pandas as pd
import numpy as np
import re
from datetime import datetime
import time
from tqdm import tqdm
# from transliterate import translit
# /stopwatch for code/
start_time = time.time()
# %%
pd.set_option('display.max_column', None)
# %%
print('Beginning dataset download...')
df = pd.read_csv('ds_dirty_fin_202410041147.csv')      # <========== Put path to ur df here
print(f'Dataset has been successfully uploaded. Size:{df.shape} \nTime: {round(time.time()-start_time,2)}s\n\n')
# %% FUNCS
def split_fio(full_fio: str) -> list:
    '''
    Takes a string of full name, and breaks it down into a name,\n
    surname and patronymic
    '''
    if re.search(r'[0-9]', full_fio):
        return np.nan

    fio_list = full_fio.title().split(maxsplit=2)
    try:
        surname = fio_list[0]
    except IndexError:
        surname = np.nan

    try:
        name = fio_list[1]
    except IndexError:
        name = np.nan

    try:
        patronymic = fio_list[2]
    except IndexError:
        patronymic = np.nan
    res = [surname, name, patronymic]
    return res

def change_date(x):
    '''
    Changes the emissions in the date
    '''
    try:
        date = (datetime.now() - datetime.strptime(x, "%Y-%m-%d")).days
        if 14*365 <= date <= 100*365:
            return x
        else:
            return np.nan
    except:
        return np.nan

def replace_fio(fio):
    '''
    Change the value in the dataframe to the value from the dictionary
    '''
    return fio_dict.get(fio, fio)

def replace_name(name):
    '''
    Change the value in the dataframe to the value from the dictionary
    '''
    if isinstance(name, str):  # Проверяем, является ли значение строкой
        return names_dict.get(name, name.title())
    return name  # Возвращаем значение без изменений, если это не строка

def replace_surname(surname):
    '''
    Change the value in the dataframe to the value from the dictionary
    '''
    if isinstance(surname, str):  # Проверяем, является ли значение строкой
        return names_dict.get(surname, surname.title())
    return surname  # Возвращаем значение без изменений, если это не строка
    
def replace_patronymic(patronymic):
    '''
    Change the value in the dataframe to the value from the dictionary
    '''
    if isinstance(patronymic, str):  # Проверяем, является ли значение строкой
        return names_dict.get(patronymic, patronymic.title())
    return patronymic  # Возвращаем значение без изменений, если это не строка

def procces_data(df_full_fio: list, name_index: int) -> list:
    '''
    For example:\n\n
    name_index = 0: **surname**;\n
    name_index = 1: **name**;\n
    name_index = 2: **patronymic**.
    '''
    value_list = []
    for fio in df_full_fio:
        try:
            value = split_fio(fio)[name_index]
            if len(value)<=1:
                value_list.append(np.nan)
            else:
                if isinstance(value, str):  # Checking that it's a string
                        value = re.sub(r'/.*', '', value)
                        if '.' in value or ',' in value:
                            value = value.replace('.', '').replace(',', '')
                value_list.append(value)
        except IndexError:
            value_list.append(np.nan)
        except TypeError:
            value_list.append(np.nan)
    
    return value_list

# %%
print('Reworking and cleaning...')
clean_st_time = time.time()
# %%
# Change all skips to NaN
df = df.replace(r'^\s*$', np.nan, regex=True)
df = df.replace('', np.nan)
# %%
# Sorting columns by number of non-zero values
sorted_columns = df.notna().sum().sort_values(ascending=False).index
df = df[sorted_columns]

# %%
# Delete all lines with invalid data 
# in client_cityzen - http
df = df[~df['client_cityzen'].str.contains('http', na=False)]

# %%
# Transform to int
for column in ['addr_region', 'addr_country', 'fin_rating', 'client_child_cnt']:
    # df = df[df[column].notna()]  # Eliminate the lines with NaN
    # df[column] = df[column].astype(int)  # Convert to int
    df[column] = df[column].apply(lambda x: str(x)[:-2] if pd.notna(x) else x)

# %%
# Replace the values in the client's TIN with str
df['client_inn'] = df['client_inn'].apply(
    lambda x: str(x)[:-2] if not pd.isna(x) else x
)
# %%
# Change the values in the client's SNILS to str
df['client_snils'] = df['client_inn'].apply(
    lambda x: str(x)[:-2] if not pd.isna(x) else x
)
#%%
# Replacing all invalid birthday dates, 
# people under 14 and over 100 with NaN.
df['client_bday'] = df['client_bday'].apply(change_date)

# %% 
# Denote the list of full names from the dataset
df_full_fio = df['client_fio_full'].value_counts().index.tolist()

#%%
# Create lists for first name, last name and patronymic,
# and clean them out of the rubbish
surname_list = procces_data(df_full_fio, 0)
name_list = procces_data(df_full_fio, 1)
patronymic_list = procces_data(df_full_fio, 2)

# %%
# Create a list for full names
full_fio_list = [f"{surname} {name} {patronymic}".upper() for surname, name, patronymic in zip(surname_list, name_list, patronymic_list)]

# %%
# Create copy of df
df_res = df.copy()

# %%
fio_dict = dict(zip(df['client_fio_full'].value_counts().index.tolist(), full_fio_list))
names_dict = dict(zip(full_fio_list, name_list))
surnames_dict = dict(zip(full_fio_list, surname_list))
patronymics_dict = dict(zip(full_fio_list, patronymic_list))

# %%
df_res['client_fio_full'] = df_res['client_fio_full'].apply(replace_fio)
df_res['client_first_name'] = df_res['client_first_name'].apply(replace_name)
df_res['client_last_name'] = df_res['client_last_name'].apply(replace_surname)
df_res['client_middle_name'] = df_res['client_middle_name'].apply(replace_patronymic)

print(f'Cleanin` is complete.\nTime: {round(time.time() - clean_st_time, 2)}s\n\n')

# %%
# Переводим транслит
# Словарь для перевода транслита на кириллицу
translit_dict = {
    'a': 'а', 'b': 'б', 'v': 'в', 'g': 'г', 'd': 'д', 'e': 'е', 'yo': 'ё', 'zh': 'ж', 'z': 'з',
    'i': 'и', 'j': 'й', 'k': 'к', 'l': 'л', 'm': 'м', 'n': 'н', 'o': 'о', 'p': 'п', 'r': 'р',
    's': 'с', 't': 'т', 'u': 'у', 'f': 'ф', 'h': 'х', 'c': 'ц', 'ch': 'ч', 'sh': 'ш', 'sch': 'щ',
    'y': 'ы', 'e': 'э', 'yu': 'ю', 'ya': 'я',
    'A': 'А', 'B': 'Б', 'V': 'В', 'G': 'Г', 'D': 'Д', 'E': 'Е', 'Yo': 'Ё', 'Zh': 'Ж', 'Z': 'З',
    'I': 'И', 'J': 'Й', 'K': 'К', 'L': 'Л', 'M': 'М', 'N': 'Н', 'O': 'О', 'P': 'П', 'R': 'Р',
    'S': 'С', 'T': 'Т', 'U': 'У', 'F': 'Ф', 'H': 'Х', 'C': 'Ц', 'Ch': 'Ч', 'Sh': 'Ш', 'Sch': 'Щ',
    'Y': 'Ы', 'E': 'Э', 'Yu': 'Ю', 'Ya': 'Я'}

# Регулярное выражение для поиска слов транслитом
translit_pattern = re.compile(r"\b[a-zA-Z]{2,}\b")

def translit_to_cyrillic(text):
    if not isinstance(text, str):
        return text

    def replace_match(match):
        word = match.group(0)
        # Переводим каждую букву слова
        translated_word = "".join(translit_dict.get(char, char) for char in word)
        return translated_word

    # Заменяем все слова транслитом на кириллицу
    return translit_pattern.sub(replace_match, text)

# Список нужных столбцов
needed_columns = ["client_first_name","client_middle_name","client_last_name","client_fio_full",]

# Преобразуем значения в нужных столбцах
for column in needed_columns:
    df_res[column] = df_res[column].apply(translit_to_cyrillic)

# %%

# Merge client strings into one in dataframe golden_df
df_res = df_res.sort_values(by='update_date', ascending=False)

golden_df = df_res.groupby('client_fio_full').first().reset_index()

for col in tqdm(df_res.columns, desc="Merging strings by columns"):
    if col != 'client_fio_full':
        golden_df[col] = golden_df[col].combine_first(df.groupby('client_fio_full')[col].last())
# %%
print('The merger is complete!')
golden_df.to_csv('golden_df.csv')
print(f'Shape: {golden_df.shape}')
print('Save file...')
print('File saved in: golden_df.csv\n')
# %%
# /stopwatch end/
end_time = time.time()
elapsed_time = round(end_time-start_time, 2)
print(f'Code execution time: {elapsed_time}s ({elapsed_time/60}min)')

