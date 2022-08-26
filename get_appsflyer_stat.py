# Скрипт получает данные по API из аналитического сервиса AppsFlyer
# После получения данных скрипт объединяет два отчета со статистикой и делает необходимые преобразования
# После этого данные записываются в базу данных


import requests
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import timedelta, datetime, date
from clickhouse_driver.client import Client
import numpy as np
import io
import json
import logging
import config_lm


ch_engine_db = config_lm.lm_ch_con_string + '/agg'
telegram_token = config_lm.baton_bot_token
chat_id = config_lm.lm_tg_chat 
table = 'appsflyer_events'
query_optimaze = f"""OPTIMIZE TABLE {table} FINAL"""


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


def success_message(table):
    now = datetime.now().strftime("<b> %d/%m/%Y %H:%M:%S </b>")
    logger.info(now)
    msg = 'Таск отработал! '+f'<b>({table})</b> ' + now
    logger.info(msg) 
    send_message(msg)


def fail_message(table):
    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    logger.info(now) 
    msg = '❌ Task fail! '+f'<b>({table})</b> ' + now
    logger.info(msg)
    send_message(msg)


def get_logger(name):
    logger = logging.getLogger(name)
    if not logger.handlers:
        # Prevent logging from propagating to the root logger
        logger.propagate = 0
        console = logging.StreamHandler()
        logger.addHandler(console)
        formatter = logging.Formatter('%(asctime)s \t%(levelname)s \t%(name)s \t%(message)s')
        console.setFormatter(formatter)
    return logger

def get_last_date_of_table(table, con):
    # Запрашиваем последнюю дату из бд.
    max_bd_date = pd.read_sql(text(f"""SELECT MAX(event_time) as max_date FROM {table}"""), con)
    #print(max_bd_date, max_bd_date.dtypes)
    #max_bd_date['max_date'] = max_bd_date['max_date'].dt.tz_convert('Europe/Moscow').dt.tz_localize(None)
    max_bd_date = max_bd_date.loc[0, 'max_date']
    return max_bd_date


def get_columns_type_df(table, con):
    # Запрашивает описание таблицы,
    # оставляет только название колонок и тп данных
    query = f"""DESC TABLE (SELECT * FROM {table})"""
    df = pd.read_sql(text(query), con)
    df = df[['name', 'type']]
    return df


def get_data(app_list, report_type, api_token, start, end):
    result = pd.DataFrame()
    params = {
        'api_token': f'{api_token}',
        'from': f'{start}',
        'to': f'{end}',
        'timezone': 'Europe/Moscow'}
    for app_id in app_list:
        for i in report_type:
            request_url = f'https://hq.appsflyer.com/export/{app_id}/{i}/v5'
            res = requests.request('GET', request_url, params=params)
            if res.status_code != 200:
                if res.status_code == 404:
                    print('There is a problem with the request URL. Make sure that it is correct')
                else:
                    print('There was a problem retrieving data: ', res.text)
                print(res)
            else:
                rawData = pd.read_csv(io.StringIO(res.text))
                result = result.append(rawData, ignore_index=True)
                get_cnt_str = len(result)
        # file = open("resp_text.txt", "w")
        # file.write(res.text)
        # file.close()
    return result, get_cnt_str


def transform(df):
    name_for_bd = ['attributed_touch_type', 'attributed_touch_time', 'install_time',
           'event_time', 'event_name', 'event_value', 'event_revenue',
           'event_revenue_currency', 'event_revenue_usd', 'event_source',
           'is_receipt_validated', 'partner', 'media_source', 'channel',
           'keywords', 'campaign', 'campaign_id', 'adset', 'adset_id', 'ad',
           'ad_id', 'ad_type', 'site_id', 'sub_site_id', 'sub_param_1',
           'sub_param_2', 'sub_param_3', 'sub_param_4', 'sub_param_5',
           'cost_model', 'cost_value', 'cost_currency', 'contributor_1_partner',
           'contributor_1_media_source', 'contributor_1_campaign',
           'contributor_1_touch_type', 'contributor_1_touch_time',
           'contributor_2_partner', 'contributor_2_media_source',
           'contributor_2_campaign', 'contributor_2_touch_type',
           'contributor_2_touch_time', 'contributor_3_partner',
           'contributor_3_media_source', 'contributor_3_campaign',
           'contributor_3_touch_type', 'contributor_3_touch_time', 'region',
           'country_code', 'state', 'city', 'postal_code', 'dma', 'ip', 'wifi',
           'operator', 'carrier', 'language', 'appsflyer_id', 'advertising_id',
           'idfa', 'android_id', 'customer_user_id', 'imei', 'idfv', 'platform',
           'device_type', 'os_version', 'app_version', 'sdk_version', 'app_id',
           'app_name', 'bundle_id', 'is_retargeting',
           'retargeting_conversion_type', 'attribution_lookback',
           'reengagement_window', 'is_primary_attribution', 'user_agent',
           'http_referrer', 'original_url']
    df.columns = name_for_bd
    df[['af_content_id']] = np.nan
    for i in df.index:
        try:
            temp_df = json.loads(df['event_value'][i])
            df.loc[i, 'af_content_id'] = temp_df['af_content_id']
        except:
            df.loc[i, 'af_content_id'] = np.nan
    # df['event_hash'] = df.apply(lambda x: hash(tuple(x)), axis = 1)
    df['attributed_touch_type'] = df['attributed_touch_type'].astype(str)
    df['event_name'] = df['event_name'].astype(str)
    df['event_value'] = df['event_value'].astype(str)
    df['event_revenue'] = round(df['event_revenue'].astype('float'), 4)
    df['event_revenue_currency'] = df['event_revenue_currency'].astype(str)
    df['event_revenue_usd'] = round(df['event_revenue_usd'].astype('float'), 4)
    df['event_source'] = df['event_source'].astype(str)
    df['is_receipt_validated'] = df['is_receipt_validated'].astype(str)
    df['partner'] = df['partner'].astype(str)
    df['channel'] = df['channel'].astype(str)
    df['keywords'] = df['keywords'].astype(str)
    df['campaign'] = df['campaign'].astype(str)
    df['campaign_id'] = df['campaign_id'].astype(str)
    df['adset'] = df['adset'].astype(str)
    df['adset_id'] = df['adset_id'].astype(str)
    df['ad'] = df['ad'].astype(str)
    df['ad_id'] = df['ad_id'].astype(str)
    df['ad_type'] = df['ad_type'].astype(str)
    df['site_id'] = df['site_id'].astype(str)
    df['sub_site_id'] = df['sub_site_id'].astype(str)
    df['sub_param_1'] = df['sub_param_1'].astype(str)
    df['sub_param_2'] = df['sub_param_2'].astype(str)
    df['sub_param_3'] = df['sub_param_3'].astype(str)
    df['sub_param_4'] = df['sub_param_4'].astype(str)
    df['sub_param_5'] = df['sub_param_5'].astype(str)
    df['cost_model'] = df['cost_model'].astype(str)
    df['cost_value'] = df['cost_value'].astype(str)
    df['cost_currency'] = df['cost_currency'].astype(str)
    df['contributor_1_partner'] = df['contributor_1_partner'].astype(str)
    df['contributor_1_media_source'] = df['contributor_1_media_source'].astype(str)
    df['contributor_1_campaign'] = df['contributor_1_campaign'].astype(str)
    df['contributor_1_touch_type'] = df['contributor_1_touch_type'].astype(str)
    df['contributor_1_touch_time'] = df['contributor_1_touch_time'].astype(str)
    df['contributor_2_partner'] = df['contributor_2_partner'].astype(str)
    df['contributor_2_media_source'] = df['contributor_2_media_source'].astype(str)
    df['contributor_2_campaign'] = df['contributor_2_campaign'].astype(str)
    df['contributor_2_touch_type'] = df['contributor_2_touch_type'].astype(str)
    df['contributor_2_touch_time'] = df['contributor_2_touch_time'].astype(str)
    df['contributor_3_partner'] = df['contributor_3_partner'].astype(str)
    df['contributor_3_media_source'] = df['contributor_3_media_source'].astype(str)
    df['contributor_3_campaign'] = df['contributor_3_campaign'].astype(str)
    df['contributor_3_touch_type'] = df['contributor_3_touch_type'].astype(str)
    df['contributor_3_touch_time'] = df['contributor_3_touch_time'].astype(str)
    df['region'] = df['region'].astype(str)
    df['country_code'] = df['country_code'].astype(str)
    df['state'] = df['state'].astype(str)
    df['city'] = df['city'].astype(str)
    df['postal_code'] = df['postal_code'].astype(str)
    df['dma'] = df['dma'].astype(str)
    df['ip'] = df['ip'].astype(str)
    df['wifi'] = df['wifi'].astype(str)
    df['operator'] = df['operator'].astype(str)
    df['carrier'] = df['carrier'].astype(str)
    df['language'] = df['language'].astype(str)
    df['appsflyer_id'] = df['appsflyer_id'].astype(str)
    df['advertising_id'] = df['advertising_id'].astype(str)
    df['idfa'] = df['idfa'].astype(str)
    df['android_id'] = df['android_id'].astype(str)
    df['customer_user_id'] = df['customer_user_id'].astype(str)
    df['imei'] = df['imei'].astype(str)
    df['idfv'] = df['idfv'].astype(str)
    df['platform'] = df['platform'].astype(str)
    df['device_type'] = df['device_type'].astype(str)
    df['os_version'] = df['os_version'].astype(str)
    df['app_version'] = df['app_version'].astype(str)
    df['sdk_version'] = df['sdk_version'].astype(str)
    df['app_id'] = df['app_id'].astype(str)
    df['app_name'] = df['app_name'].astype(str)
    df['bundle_id'] = df['bundle_id'].astype(str)
    df['is_retargeting'] = df['is_retargeting'].astype(str)
    df['retargeting_conversion_type'] = df['retargeting_conversion_type'].astype(str)
    df['attribution_lookback'] = df['attribution_lookback'].astype(str)
    df['reengagement_window'] = df['reengagement_window'].astype(str)
    df['is_primary_attribution'] = df['is_primary_attribution'].astype(str)
    df['user_agent'] = df['user_agent'].astype(str)
    df['http_referrer'] = df['http_referrer'].astype(str)
    df['original_url'] = df['original_url'].astype(str)
    df['original_url'] = df['original_url'].astype(str)
    df['af_content_id'] = df['af_content_id'].astype(str)
    # df['event_hash'] = df['event_hash'].astype(str)
    df['updated_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df = df.replace('Nan', np.nan)
    df = df.replace('nan', np.nan)
    df = df.replace("None", np.nan)
    df = df.replace('NaT', np.nan)
    return df

def load_to_db(df, table, con):
    df.to_sql(table, con, if_exists='append', index=False)


def get_table(table, con):
    # Запрашиваем последнюю дату из бд.
    df = pd.read_sql(text(f"""SELECT * FROM {table}"""), con)
    return df


def optimaze_table(q, client):
    client.execute(q)


def process():
    app_list = config_lm.appid_list #список приложений
    report_type = ['in_app_events_report', 'organic_in_app_events_report']
    api_token = config_lm.appsflyer_api_token
    # start = '2022-05-04'
    # end = '2022-05-06'
    end = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    max_bd_date = get_last_date_of_table(table, con=ch_engine_db)
    logger.info(f'Последняя дата в базе {max_bd_date}')
    start = (max_bd_date - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")

    logger.info(f'Получаю данные с {start} по {end}')
    df, get_cnt_str = get_data(app_list, report_type, api_token, start, end)
    # df.to_csv('df.csv', index=False)
    # df = pd.read_csv('result.csv')
    logger.info(f'Получил данные с {start} по {end}. Получил {get_cnt_str} строк')
    df = transform(df)
    logger.info(f'Обработал данные')
    load_to_db(df, table, con=ch_engine_db)
    logger.info(f'Записал данные в {table}')
    optimaze_table(q=query_optimaze, client=client)
    logger.info(f'Оптимизировал данные в {table}')


if __name__ == '__main__':
    logger = get_logger('lm_stat')
    logger.setLevel(logging.INFO)
    try:
        process()
        # success_message(table)
    except Exception as e:
        logger.error(f'Task fail - {e}')
        fail_message(table)
        raise