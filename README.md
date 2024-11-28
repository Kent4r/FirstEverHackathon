# Hackathon By "физтех🤘😈🤟🤟💪😎💪"

## Доброе утро, это неформальная документация к нашему решению по первому треку (золотая запись)

### Для разработки процесса нам понадобятся два этапа:

1. Загуглить и понять, что такое "Золотая Запись".
2. Проанализировать данные, находящиеся в данном нам датасете.

### Так ну теперь собственно загуглив, что такое "Золотая запись", у нас выстроился текущий план:

1. Сгруппировать данные по ФИО, для соединения в золотую запись.
2. Данные брать по актуальности или по тяжелым расчетам веса правдивости источников.
3. Брать данные по праведности источников, но для такого варианта тупо не хватает времени, так как такой метод для нас в новинку и на его разработку уйдёт минимум пару дней.

### Проанализировав данные, мы выяснили, что:

1. Данные могут быть написаны транслитом.
2. Среди записей есть строки, НЕ относящиеся к общей теме датасета.
3. Даты могут быть некорректными, например возраст >100 или день рождения в нулевое число месяца.
4. Есть явные выбросы, например посреди строковой ячейки данных могут быть числа или спец.символы или в ФИО указана страна или доп.сведения.
5. Данные по типу СНИЛС или ИНН можно подогнать под маску, для отсеивания неявных неправдивых данных.

### Сделав выводы с двух этапов выше, мы составили план процесса:

1. Чистка данных:
	1. Заменяем в колонке с ФИО все слова и символы, содержащие спец.символы, на NaN.
	2. Подгоняем всё, что можно, по маске.
	3. Заменяем все ФИО с латиницей на кириллицу.
	4. Удаляем даты, где человек старше 100 и младше 14 лет.
	5. Для нахождения несвязанных записей, производим поиск по ключу "http" и удаляем записи, содержащие его.
2. Группировка по ФИО: Находим уникальные записи и записываем их в отдельный файл для удобного слияния.
3. Merge(слияние) записей в золотые: Берём самую актуальную запись как за основу и засовываем в неё данные из других записей, которых нет в основной.

### Таким образом решается вопрос самых уникальных данных, но уже без выбросов и некорректных данных, т.е. проблема акутальности решена полностью, а проблема праведности не до конца.

## Ссылки

Презентация - либо Презентация.pptx, либо https://disk.yandex.ru/i/Sdsu0CrQ3H9AFA

Видео демки - в папке на гугл диске https://drive.google.com/drive/folders/1aG69s-3ErsinGAZlTMqFchZcRqH5bmEz?usp=sharing 
Текущая версия демо - v0.2

Выходной CSV файл либо здешний golden_df.csv, либо при наличии на гугл диске по ссылке выше
