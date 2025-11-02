import pandas as pd
import numpy as np
import datetime
import requests
from bs4 import BeautifulSoup
import re

df_old = pd.read_csv('history_auction.csv')

# Определяем последний аукцион в старом файле
df_old['date_close_auction_date'] = pd.to_datetime(df_old['date_close_auction'])
last_auction = df_old[df_old['date_close_auction_date']==df_old['date_close_auction_date'].max()]['auction'].iloc[0].split('/')[-1]
last_auction = int(last_auction)

# Удаляем временный столбец
df_old.drop(columns='date_close_auction_date', inplace=True, axis=1)

# Определяем текущие аукционы на сайте
req = requests.get("https://www.wolmar.ru")
soup = BeautifulSoup(req.text, "html.parser")
auction_links = soup.select("a.current, a.current_dark")
links = [a["href"] for a in auction_links if a.get("href")]
auction_numbers = [int(au.split('/')[-1]) for au in links]
min_current_auction = min(auction_numbers)

auction = []
close_auction = [] 
name_lot = []
year_lot = []
material_lot = []
quality_lot = []
last_bet = []
drop_auction_error = ''
# Добавляем новые аукционы в датафрейм до последнего закрытого
try:
    for number_auction in range(last_auction+1,min_current_auction):
        req = requests.get("https://www.wolmar.ru/auction/"+str(number_auction))
        soup = BeautifulSoup(req.text, "html.parser")
        last_auction_in_loop = "https://www.wolmar.ru/auction/"+str(number_auction)
        if re.search(r'(VIP)', soup.find('h1').text) is not None or re.search(r'(Standart)', soup.find('h1').text) is not None:
            date_close = soup.find('h1').find('span').text[8:18]
            print(date_close)
        
            for i in soup.find('ul').findAll('li')[::-1]:
                last_page = i.text
                break

            for page in range(1,int(last_page)+1):
                page_req = requests.get("https://www.wolmar.ru/auction/"+str(number_auction)+"?page="+str(page))
                page_soup = BeautifulSoup(page_req.text, "html.parser")
            
                for i in page_soup.findAll('table')[4].findAll('tr')[3:]:
                    if i.findAll('td')[1].find('a') is not None:
                        auction.append("https://www.wolmar.ru/auction/"+str(number_auction))
                        close_auction.append(date_close)  
                        name_lot.append(i.findAll('td')[1].find('a').text)
                        year_lot.append(i.findAll('td')[2].text)
                        material_lot.append(i.findAll('td')[4].text)
                        quality_lot.append(i.findAll('td')[5].find('nobr').text)
                        last_bet.append(i.findAll('td')[8].text)
except Exception as e:
    # Если произошла ошибка, сохраняем номер аукциона, на котором это случилось
    drop_auction_error = last_auction_in_loop

df = pd.DataFrame({'auction': auction,
                   'date_close_auction':close_auction,
                   'name':name_lot,
                   'year': year_lot,
                   'material':material_lot,
                   'quality': quality_lot,
                   'last_bet':last_bet})

# Если была ошибка, удаляем данные проблемного аукциона

df = df[df['auction']!=drop_auction_error]

if len(df)!=0:
    df_new = pd.concat([df_old,df])
# Сохраняем обновленный датафрейм в CSV
    df_new.to_csv('history_auction.csv',
                  index=False)
else:
    df_new = df_old

# Анализируем монеты после 1989 года
df_coins_after_1989 = df_new[~df_new['year'].isna()]
df_coins_after_1989['year'] = df_coins_after_1989['year'].astype('int')
df_coins_after_1989 = df_coins_after_1989[df_coins_after_1989['year']>1989]

# Преобразуем столбцы к нужным типам
df_coins_after_1989['date_close_auction'] = pd.to_datetime(df_coins_after_1989['date_close_auction'],
                                                           format='%d.%m.%Y').dt.date
df_coins_after_1989['last_bet'] = df_coins_after_1989['last_bet'].replace(r'\s+','',regex=True).astype('int')

# Фильтруем по материалу (только Au и Ag)
df_coins_after_1989['material_2'] = np.where(df_coins_after_1989['material'].str.startswith('Au', na=False),
                                            'Au',
                                            '-')
df_coins_after_1989['material_2'] = np.where(df_coins_after_1989['material'].str.startswith('Ag', na=False),
                                             'Ag',
                                             df_coins_after_1989['material_2'])
df_coins_after_1989_ag_au = df_coins_after_1989[df_coins_after_1989['material_2']!='-']

# Очистка столбца 'quality' от лишних символов
df_coins_after_1989_ag_au['quality'] = df_coins_after_1989_ag_au['quality'].apply(lambda x: x.replace("\t", ""))
df_coins_after_1989_ag_au['quality'] = df_coins_after_1989_ag_au['quality'].apply(lambda x: x.replace("\n", ""))

# Группировка и агрегация данных за все время и за последний год
gr_coins_material_1_all = df_coins_after_1989_ag_au.groupby(['name', 'year', 'quality', 'material', 'material_2'], 
                                                        as_index=False).last_bet.agg(last_bet_mean_all ='mean',
                                                                                     last_bet_min_all = 'min',
                                                                                     last_bet_max_all = 'max',
                                                                                     count_all = 'count')


gr_coins_material_1_year = df_coins_after_1989_ag_au[df_coins_after_1989_ag_au['date_close_auction']>datetime.datetime.strptime('2024-07-01', '%Y-%m-%d').date()].groupby(['name', 'year', 'quality', 'material', 'material_2'], 
                                                        as_index=False).last_bet.agg(last_bet_mean_year ='mean',
                                                                                     last_bet_min_year = 'min',
                                                                                     last_bet_max_year = 'max',
                                                                                     count_year = 'count')

df_info_coins = pd.merge(gr_coins_material_1_all, 
                         gr_coins_material_1_year, 
                         on=['name','year', 'quality', 'material' , 'material_2'], 
                         how="left")

auction = []
id_in_auction = []
name_lot = []
year_lot = []
material_lot = []
quality_lot = []
last_bet = []

# Сбор данных по текущем аукционам
for number_auction in auction_numbers:
    req = requests.get("https://www.wolmar.ru/auction/"+str(number_auction))
    soup = BeautifulSoup(req.text, "html.parser")

    if re.search(r'(VIP)', soup.find('h1').text) is not None or re.search(r'(Standart)', soup.find('h1').text) is not None:
        
        for i in soup.find('ul').findAll('li')[::-1]:
            last_page = i.text
            break

        for page in range(1,int(last_page)+1):
            page_req = requests.get("https://www.wolmar.ru/auction/"+str(number_auction)+"?page="+str(page))
            page_soup = BeautifulSoup(page_req.text, "html.parser")
            
            for i in page_soup.findAll('table')[4].findAll('tr')[3:]:
                if i.findAll('td')[1].find('a') is not None:
                    auction.append("https://www.wolmar.ru/auction/"+str(number_auction))
                    id_in_auction.append(i.findAll('td')[0].text)
                    name_lot.append(i.findAll('td')[1].find('a').text)
                    year_lot.append(i.findAll('td')[2].text)
                    material_lot.append(i.findAll('td')[4].text)
                    quality_lot.append(i.findAll('td')[5].find('nobr').text)
                    last_bet.append(i.findAll('td')[8].text)

# Создаем датафрейм для текущего аукциона
df_current_auction = pd.DataFrame({'auction': auction,
                   'id' : id_in_auction,
                   'name':name_lot,
                   'year': year_lot,
                   'material':material_lot,
                   'quality': quality_lot,
                   'last_bet':last_bet})

# Анализируем монеты после 1989 года в текущем аукционе
df_current_auction_after_1989 = df_current_auction[df_current_auction['year']!='']
df_current_auction_after_1989['year'] = df_current_auction_after_1989['year'].astype('int')
df_current_auction_after_1989 = df_current_auction_after_1989[df_current_auction_after_1989['year']>1989]
df_current_auction_after_1989['last_bet'] = df_current_auction_after_1989['last_bet'].replace(r'\s+','',regex=True).astype('int')

df_current_auction_after_1989['material_2'] = np.where(df_current_auction_after_1989['material'].str.startswith('Au', na=False),
                                                       'Au',
                                                       '-')
df_current_auction_after_1989['material_2'] = np.where(df_current_auction_after_1989['material'].str.startswith('Ag', na=False),
                                                       'Ag',
                                                       df_current_auction_after_1989['material_2'])
df_current_auction_after_1989 = df_current_auction_after_1989[df_current_auction_after_1989['material_2']!='-']

df_current_auction_after_1989['quality'] = df_current_auction_after_1989['quality'].apply(lambda x: x.replace("\t", ""))
df_current_auction_after_1989['quality'] = df_current_auction_after_1989['quality'].apply(lambda x: x.replace("\n", ""))

df_current_auction_after_1989_info = pd.merge(df_current_auction_after_1989, 
                                              df_info_coins, 
                                              on=['name','year', 'quality', 'material', 'material_2'], 
                                              how="left")

df_current_auction_after_1989_info['price_less_min_y'] = np.where((df_current_auction_after_1989_info['last_bet'] / df_current_auction_after_1989_info['last_bet_min_year']) < 0.8,
                                                                  1,
                                                                  0)

df_current_auction_after_1989_info['price_less_max_y'] = np.where((df_current_auction_after_1989_info['last_bet'] / df_current_auction_after_1989_info['last_bet_max_year']) < 0.8,
                                                                  1,
                                                                  0)

df_current_auction_after_1989_info.to_excel('current_auct.xlsx',
                                             sheet_name='Sheet_name_1',
                                             index=False)