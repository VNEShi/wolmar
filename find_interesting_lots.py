import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
import requests

# Загрузка интересующих монет
df = pd.read_csv('interesting_coins_for_me.csv')
df['name_as_auction'] = df['nominal']+'. '+df['cname']
df = df[df['Интерес']==1]

# Загрузка текущих аукционов
df_current_auction = pd.read_excel('current_auct.xlsx')

# Формирование датафрейма для бота
df_for_bot = df_current_auction.merge(df, 
                         left_on='name', 
                         right_on='name_as_auction', 
                         how='inner')[['name', 'sname', 'auction', 'price_less_min_y', 
                                       'price_less_max_y', 'last_bet_min_year',	'last_bet_max_year']]

df_for_bot = df_for_bot.sort_values(by='price_less_min_y', ascending=True)

# Отправка сообщений в Telegram
load_dotenv() 

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

def send_message(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text}
    response = requests.post(url, data=payload)
    if response.status_code != 200:
        print("Ошибка:", response.text)

for index, row in df_for_bot.iterrows():
    message = f"Монета: {row['name']} ({row['sname']})\nАукцион: {row['auction']}\nЦена меньше мин: {row['price_less_min_y']}\nЦена меньше макс: {row['price_less_max_y']}\nПоследняя ставка мин год: {row['last_bet_min_year']}\nПоследняя ставка макс год: {row['last_bet_max_year']}"
    send_message(message)       