# Скрипт использовался для получения курса валют по API с сайта openexchangerates.org. 
# После получения данных происходила запись в базу данных. 
# Скрипт работал по расписанию.

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import timedelta, datetime, date
from clickhouse_driver.client import Client
import numpy as np
import logging
import requests
import json
import config_lm


telegram_token = config_lm.baton_bot_token
chat_id = config_lm.lm_tg_chat

ch_engine = config_lm.lm_ch_con_string+'/agg'


def get_logger(name):
    LOGGING_FORMAT = '%(asctime)s \t%(levelname)s \t%(processName)s \t%(message)s'
    logger = logging.getLogger(name)
    if not logger.handlers:
        # Prevent logging from propagating to the root logger
        logger.propagate = 0
        console = logging.StreamHandler()
        logger.addHandler(console)
        formatter = logging.Formatter(LOGGING_FORMAT)
        console.setFormatter(formatter)
    return logger


client = Client(host=config_lm.lm_ch_host,
                user=config_lm.lm_db_user,
                password=config_lm.lm_db_pass,
                database='agg')


def send_message(message, chat_id=chat_id, token_id=telegram_token):
    # Функция отвечающая за отправку сообщения телеграм ботом
    r = requests.post(f'https://api.telegram.org/bot{token_id}/sendMessage',
                      data={
                          'chat_id': chat_id,
                          'parse_mode': 'html',
                          'text': message},
                      timeout=100)
    return r.status_code


def message(msg):
    now = datetime.now().strftime("<b> %d/%m/%Y %H:%M:%S </b>")
    logger.info(now)
    logger.info(msg)
    send_message(msg)


def load_to_db(df, table, con):
    df.to_sql(table, con, if_exists='append', index=False)


def get_data_range(date_start, date_end):
    df = pd.DataFrame(columns=['from', 'to', 'date', 'value'])
    date_s = datetime.strptime(date_start, "%Y-%m-%d")
    date_e = datetime.strptime(date_end, "%Y-%m-%d")
    while date_s <= date_e:
        date_res = date_s.strftime("%Y-%m-%d")
        response = requests.get(f'https://openexchangerates.org/api/historical/{date_res}.json?app_id=56c757c02cdf40c5bf988b31c32db80b&base=USD')
        values = json.loads(response.text)
        values = values['rates']
        result = pd.DataFrame(values.items(), columns=['to', 'value'])
        result['date'] = date_s.strftime("%Y-%m-%d")
        result['from'] = 'USD'
        result['updated_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df = df.append(result, ignore_index=True)
        date_s = date_s + timedelta(days=1)
    df['value'] = round(df['value'].astype('float'), 4)
    df = df.append(df.iloc[0], ignore_index=True)
    cnt_str = len(df)
    return df, cnt_str


def get_data(date_start):
    df = pd.DataFrame(columns=['from', 'to', 'date', 'value'])
    date_s = datetime.strptime(date_start, "%Y-%m-%d")
    date_res = date_s.strftime("%Y-%m-%d")
    response = requests.get(f'https://openexchangerates.org/api/historical/{date_res}.json?app_id=56c757c02cdf40c5bf988b31c32db80b&base=USD')
    values = json.loads(response.text)
    values = values['rates']
    result = pd.DataFrame(values.items(), columns=['to', 'value'])
    result['date'] = date_s.strftime("%Y-%m-%d")
    result['from'] = 'USD'
    result['updated_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df = df.append(result, ignore_index=True)
    date_s = date_s + timedelta(days=1)
    df['value'] = round(df['value'].astype('float'), 4)
    df = df.append(df.iloc[0], ignore_index=True)
    cnt_str = len(df)
    return df, cnt_str


def process(table, date_start, date_end, con_to):
    # logger.info(f'Начинаю получать данные с {date_start} по {date_end}')
    # df, cnt_str = get_data_range(date_start, date_end)
    logger.info(f'Начинаю получать данные за {date_start}')
    df, cnt_str = get_data(date_start)
    logger.info(f'Получил {cnt_str} строк. Начинаю загружать')
    load_to_db(df, table, con=con_to)
    logger.info(f'Данные загружены!')


if __name__ == '__main__':
    logger = get_logger('LM')
    logger.setLevel(logging.INFO)
    try:
        process(table='rate_currency', date_start=datetime.now().strftime("%Y-%m-%d"), date_end='', con_to=ch_engine)
    except Exception as e:
        logger.error(f'Task fail - {e}')
        msg = f'❌ Task fail! (agg.rate_currency)'
        message(msg=msg)
        raise