#%%
# IMPORTS
import pandas as pd
import numpy as np
import re
from datetime import datetime
# %%
# Читаем датасет
df = pd.read_csv('ds_dirty_fin_202410041147.csv')

# %%
# Замена всех пропусков на NaN
df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
df.replace('', np.nan, inplace=True)

# %%
# Функции
def split_fio(full_fio): # При помощи регулярок сносим хрень в ФИО
    if re.search(r'[0-9]', full_fio):
        return None

    fio_list = full_fio.title().split(maxsplit=2)
    try: surname = fio_list[0]
    except IndexError: surname = None

    try: name = fio_list[1]
    except IndexError: name = None

    try: patronymic = fio_list[2]
    except IndexError: patronymic = None
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

surname_list = []
name_list = []
patronymic_list = []
for part in fio_parts:
    surname_list.append(part[0])
    name_list.append(part[1])
    patronymic_list.append(part[2])

#full_fio_list = [f"{surname} {name} {patronymic}".upper() for surname, name, patronymic in [surname_list, name_list, patronymic_list]]
full_fio_list = []
for i in range(len(surname_list)):
    full_fio_list.append(f"{surname_list[i]} {name_list[i]} {patronymic_list[i]}")
# %%
# Размечаем замены
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
    golden_df[column] = golden_df[column].apply(translit_to_cyrillic)
# %%
# Вывод
golden_df.to_csv("test_res.csv")
# %% RUN ABOVE RUN ABOVE RUN ABOVE RUN ABOVE RUN ABOVE RUN ABOVE RUN ABOVE
golden_df.shape
# %%
golden_df.head()
# %%
