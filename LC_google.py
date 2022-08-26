# Скрипт с помощью нескольких запросов получает информацию по партнерам, определенным образом обрабатывает ее и записывает в Google таблицу
# Скрипт работает по расписанию и в соответствии с расписанием актуализирует выгрузку

import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from clickhouse_driver import Client
import numpy as np
import logging
import pygsheets
import requests
import config


ch_engine = config.clickhouse_conn
pg_la_dsn = config.postgres_conn
sheet_id = config.sheet_id
gid_id = '0'
telegram_token = config.telegram_token
chat_id = config.chat_id


def send_message(message, chat_id=chat_id, token_id=telegram_token):
    # Функция отвечающая за отправку сообщения телеграм ботом
    r = requests.post(f'https://api.telegram.org/bot{token_id}/sendMessage',
                      data={
                          'chat_id': chat_id,
                          'parse_mode': 'html',
                          'text': message},
                      timeout=100)
    return r.status_code


def get_logger(name):
    """ logger options
    """
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        logger.propagate = 0
        console = logging.StreamHandler()
        logger.addHandler(console)
        formatter = logging.Formatter('%(asctime)s \t%(levelname)s \t%(name)s \t%(message)s')
        console.setFormatter(formatter)
    return logger


def extract_data(sheet_id: str, gid_id: str) -> pd.DataFrame:
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid_id}"
    # col_names = ['email',]
    df = pd.read_csv(url)
    df = df.rename(columns={'email': 'email_in_source'})
    logger.info(f'Выгружено данных - %s', len(df))
    return df


def get_query():
    lf_q = f"""
            SELECT
                usr_lf.user_id as user_id,
                users.email as email,
                toDate(usr_lf.created_at) as reg_date,
                if(first_date='1970-01-01', NULL ,first_date) as first_date,
                usr_lf.utm_campaign as utm_campaign,
                round(web_sts.web_money, 2) as money
            from (
            SELECT
                user_id,
                created_at,
                JSONExtractString(replaceAll(query_params, '''', '"'), 'utm_campaign') as utm_campaign
            from luckyfeed.user_registration_data urd 
            where user_id in (SELECT toUInt64(id) as id from luckyfeed.users)) usr_lf
            left join (
                SELECT	
                    user_id,
                    sum(cpc_block_profit_with_commission)+sum(postbacks_confirmed_money_with_commission) as web_money
                from luckyfeed.v_daily_stats_full_v2
                where date >= '2022-03-23'
                group by user_id ) web_sts
            on usr_lf.user_id = web_sts.user_id
            left join luckyfeed.users on users.id = usr_lf.user_id
            left join (
            SELECT
                user_id,
                min(`date`) as first_date
            from luckyfeed.v_daily_stats_full_v2
            group by user_id) as activ
            on usr_lf.user_id = activ.user_id"""
    la_q = f"""
            SELECT
                usr_la.user_id as user_id,
                users.email as email,
                toDate(usr_la.created_at) as reg_date,
                usr_la.utm_campaign as utm_campaign,
                round(adv_sts.adv_money, 2) as money
            from (
            SELECT
                toUInt64(user_id) as user_id,
                created_at,
                JSONExtractString(replaceAll(query_params, '''', '"'), 'utm_campaign') as utm_campaign
            from ads.user_registration_data urd 
            where user_id in (SELECT toUInt64(id) as id from ads.users)) usr_la
            left join (
                SELECT	
                    advertiser_id,
                    sum(advertiser_money) as adv_money
                from ads.v_daily_advertisers vda
                group by advertiser_id) adv_sts
            on usr_la.user_id = adv_sts.advertiser_id
            left join ads.users on users.id = usr_la.user_id"""
    la_first_transaction = f"""
            select
                min(date) as first_date,
                owner_id as user_id,
                email
            from (
            select
                operations.transaction_id,
                requests.type_category_id,
                request_type_categories.name,
                account_id,
                account_owner_types.name owner_type,
                currencies.index currencie,
                amount_diff,
                operations.created_at::Date as date,
                transactions.type_id,
                transaction_types.name as transactions_type,
                transactions.comment,
                owner_id,
                users.email
            FROM bookkeeping.operations
            inner join bookkeeping.transactions on operations.transaction_id = transactions.id 
            inner join bookkeeping.accounts on operations.account_id = accounts.id and accounts.owner_type_id != 21
            inner join users.users on accounts.owner_id = users.id
            inner join bookkeeping.transaction_types on transactions.type_id = transaction_types.id
            inner join bookkeeping.account_owner_types on accounts.owner_type_id = account_owner_types.id
            inner join bookkeeping.currencies on accounts.currency_id = currencies.id
            left join cashbox.requests on operations.transaction_id = requests.transaction_id
            left join cashbox.request_type_categories on request_type_categories.id = requests.type_category_id) as stat
            where type_id in (1, 15)
            group by
                user_id,
                email"""
    lo_q = f"""
            SELECT
                usr_lo.pub_id as user_id,
                usr_lo.email as email,
                toDate(usr_lo.created_at) as reg_date,
                if(first_date='1970-01-01', NULL ,first_date) as first_date,
                users_utm.utm_campaign as utm_campaign,
                round(money_stat.total_money, 2) as money
            from (
            SELECT
                pub_id,
                email,
                created_at
            from online.pub_users) usr_lo
            left join online.users_utm on usr_lo.pub_id = users_utm.user_id
            left join (
                select
                pub_id as user_id,
                SUM(money) as total_money
            from(
                SELECT
                    *,
                    if(status in (2,3,4), if(currency_id = 60, amount*currency_usd, amount ), 0) as money
                from
                (SELECT
                    click_date as date,
                    pub_id,
                    status,
                    currency_id,
                    sum(amount/10000) as amount,
                    lead_id
                FROM online.stats s FINAL
                where lead_id > 0 and status in (2,3,4)
                group by
                    click_date,
                    pub_id,
                    currency_id,
                    status,
                    lead_id) sts
                any left join default.v_usd_currency_rate vucr
                using date)
            group by
                user_id) as money_stat
            on usr_lo.pub_id = money_stat.user_id
            left join (
                SELECT
                    pub_id,
                    min(click_date) as first_date
                from online.stats
                group by pub_id) as active
            on usr_lo.pub_id = active.pub_id"""
    return lf_q, la_q, lo_q, la_first_transaction


def get_data(q, con):
    df = pd.read_sql(q, con)
    cnt_str = len(df)
    logger.info(f'Получил {cnt_str} строк из базы')
    return df, cnt_str


def transform_data_lo_lf(df1, df2):
    df1 = df1.iloc[::, 0]
    df1 = df1.astype(str).str.replace(' ', '')
    df1 = df1.astype(str).str.strip()
    df = df2.merge(df1, how = 'left', left_on='email', right_on='email_in_source')
    df['email_in_source'] = df['email_in_source'].fillna(0)
    df['utm_campaign'] = df['utm_campaign'].fillna('')
    df['first_date'] = df['first_date'].fillna('')
    df = df[df['email_in_source']!=0 | df['utm_campaign'].str.contains('lc_course', regex=False)].reset_index(drop=True)
    df['first_date'] = pd.to_datetime(df['first_date'])
    df = df[(df['first_date']>='2022-03-23') | (pd.isna(df['first_date'])==True)]
    df['first_date'] = df['first_date'].dt.strftime('%Y-%m-%d')
    df['first_date'] = df['first_date'].fillna('')
    df['upload_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _ = df.sort_values('money',ascending = False).reset_index(drop=True)
    _['money'] = round(_['money'], 2)
    _['money'] = _['money'].astype(str).str.replace('.', ',')
    _['email_in_source'] = _['email_in_source'].astype(str).str.replace('0', '')
    _ = _.rename(columns={'reg_date': 'дата регистрации', 'money': 'оборот', 'email_in_source': 'email в базе студентов',
     'upload_at': 'время обновления', 'first_date': 'дата первой активности'})
    logger.info(f'Трансформировали данные. Длина фрейма - %s', len(_))
    return _


def transform_data_la(df1, df2, df3):
    df1 = df1.iloc[::, 0]
    df1 = df1.astype(str).str.replace(' ', '')
    df1 = df1.astype(str).str.strip()
    df3 = df3[['user_id', 'first_date']]
    df = df2.merge(df1, how = 'left', left_on='email', right_on='email_in_source')
    df = df.merge(df3, how = 'left', on='user_id')
    df['email_in_source'] = df['email_in_source'].fillna(0)
    df['utm_campaign'] = df['utm_campaign'].fillna('')
    df = df[df['email_in_source']!=0 | df['utm_campaign'].str.contains('lc_course', regex=False)].reset_index(drop=True)
    df['first_date'] = pd.to_datetime(df['first_date'])
    df = df[(df['first_date']>='2022-03-23') | (pd.isna(df['first_date'])==True)]
    df['first_date'] = df['first_date'].dt.strftime('%Y-%m-%d')
    df['first_date'] = df['first_date'].fillna('')
    df['upload_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df = df[['user_id', 'email', 'reg_date', 'first_date', 'utm_campaign', 'money', 'email_in_source', 'upload_at']]
    _ = df.sort_values('money',ascending = False).reset_index(drop=True)
    _['money'] = round(_['money'], 2)
    _['money'] = _['money'].astype(str).str.replace('.', ',')
    _['email_in_source'] = _['email_in_source'].astype(str).str.replace('0', '')
    _ = _.rename(columns={'reg_date': 'дата регистрации', 'money': 'оборот', 'email_in_source': 'email в базе студентов',
     'upload_at': 'время обновления', 'first_date': 'дата первой активности'})
    logger.info(f'Трансформировали данные. Длина фрейма - %s', len(_))
    return _


def gsh_upload(df1, df2, df3):
    # Авторизация
    gc = pygsheets.authorize(service_file = config.source) 
    #открыть электронную таблицу Google по ключу
    sh = gc.open_by_key(config.key)
    #выберал первый лист 
    wks_1 = sh[0]
    wks_2 = sh[1]
    wks_3 = sh[2]
    #чистим листы
    wks_1.clear('A1')
    wks_2.clear('A1')
    wks_3.clear('A1')
    #обновил первый лист
    wks_1.set_dataframe(df1, (1,1))
    wks_2.set_dataframe(df2, (1,1))
    wks_3.set_dataframe(df3, (1,1))


def etl():
    extracted = extract_data(sheet_id, gid_id)
    lf_q, la_q, lo_q, la_first_transaction  = get_query()
    df_lf, _ = get_data(q=lf_q, con=ch_engine)
    df_la, _ = get_data(q=la_q, con=ch_engine)
    df_trans, _ = get_data(q=la_first_transaction, con=pg_la_dsn)
    df_lo, _ = get_data(q=lo_q, con=ch_engine)
    logger.info('Получил все данные')
    transformed_lf = transform_data_lo_lf(df1=extracted, df2=df_lf)
    transformed_la = transform_data_la(df1=extracted, df2=df_la, df3=df_trans)
    transformed_lo = transform_data_lo_lf(df1=extracted, df2=df_lo)
    logger.info('Трансформировал все данные')
    gsh_upload(df1=transformed_lf, df2=transformed_la, df3=transformed_lo)
    logger.info('Записал все данные в документ')
    logger.setLevel(logging.INFO)


if __name__ == '__main__':
    logger = get_logger('luckycenter_googlesheets_upload')
    logger.setLevel(logging.INFO)
    try:
        etl()
        logger.info('Скрипт отработал штатно')
    except Exception as e:
        logger.error(f'Task fail - {e}')
        msg = f'❌ Task fail! (luckycenter_googlesheets_upload)'
        send_message(message=msg, chat_id=chat_id, token_id=telegram_token)
        logger.info('Скрипт не отработал. Отправил алерт')
        raise