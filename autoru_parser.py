# Скрипт использовался для парсинга объявлений auto.ru
# На вход скрипту передавался файл со ссылками на объявления. Файлы можно найти и скачать здесь https://auto.ru/sitemapindex.xml
# Каждый скрипт проходит в каждое объявлоение и достает из него необхоимую информацию, после чего сохраняет ее в csv файл
# В скрипте пытался реализовать парсинг в несколько потоков

import pandas as pd
from bs4 import BeautifulSoup
import requests
import json
import re
import time
import wget
import gzip
import threading
from datetime import timedelta, datetime, date


def get_info():
    with gzip.open('sitemap_offers_cars_2.gz', 'rb') as f:
        file_content = f.read()
    soup_list = BeautifulSoup(file_content, 'lxml')
    url_list = soup_list.find_all('loc')
    return url_list
    
    
def func(df, url_list, thead, i, j):
    print (f'Запускаю поток {thead}')
    for car_url in range(i, j):
        print (f'Поток {thead}, вычисление {car_url}')
        try:
            r = requests.get(url_list[car_url].text)
        except:
            print (f'Не получается получить данные {car_url}: {url_list[car_url].text}')
            print (f'Засыпаю на 20 секунд')
            time.sleep(10)
            print (f'Проснулся. Едем дальше')
            continue
        r.encoding = 'utf-8'
        car_page_soup = BeautifulSoup(r.text, 'lxml')
        try:
            name = car_page_soup.find('h1').text
        except:
            name = '-'
        try:
            price = car_page_soup.find('span', class_='OfferPriceCaption__price').text
            price = re.sub(r'[^\d,.]', '', price)
        except:
            price = '-'
        try:
            year = car_page_soup.find('li', class_='CardInfoRow CardInfoRow_year').a.text
        except:
            year = '-'
        try:
            km = car_page_soup.find('li', class_='CardInfoRow CardInfoRow_kmAge').find_all('span')[1].text.replace(u'\xa0', ' ')
            km = re.sub(r'[^\d,.]', '', km)
        except:
            km = '-'
        try:
            city = car_page_soup.find('span', class_='MetroListPlace__regionName MetroListPlace_nbsp').text
        except:
            city = '-'
        try:
            date = car_page_soup.find('div', class_='CardHead').find_all('div')[0].find('div', class_='CardHead__infoItem CardHead__creationDate').text
        except:
            date = '-'
        full_info = {'name':name, 'price':price, 'year':year, 'km':km, 'city':city, 'date':date, 'url':url_list[car_url].text}
        df_new_row = pd.DataFrame([full_info])
        df = pd.concat([df, df_new_row], ignore_index=True)
    print (f'Поток {thead}.Закончено {j} вычислений')
    df.to_csv(f'df_{j}.csv')


def threaded(df1, df2, df3, df4, df5, url_list):
    # theads - количество потоков
    
    # делим вычисления на 4 потока
    t1 = threading.Thread(target=func, args=(df1, url_list, 1, 10000, 20000))
    t2 = threading.Thread(target=func, args=(df2, url_list, 2, 20000, 30000))
    t3 = threading.Thread(target=func, args=(df3, url_list, 3, 30000, 40000))
    t4 = threading.Thread(target=func, args=(df4, url_list, 4, 40000, 45000))
    t5 = threading.Thread(target=func, args=(df5, url_list, 5, 45000, 50000))
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t1.join()
    t2.join()
    t3.join()
    t4.join()
    t5.join()
    print('Вышел из функции с потоками')
    
    


def process():
    print(f'Время старта: {datetime.now()}')
    url_list = get_info()
    list_len = len(url_list)
    print (f'Количество ссылок: {list_len}')
    df1 = pd.DataFrame()
    df2 = pd.DataFrame()
    df3 = pd.DataFrame()
    df4 = pd.DataFrame()
    df5 = pd.DataFrame()
    threaded(df1, df2, df3, df4, df5, url_list)
    print(f'Время завершения: {datetime.now()}')


if __name__ == '__main__':
    process()