# Скрипт использовался для проверки статистики и алертинга в Телеграмм при достижении сайтом некоторых порогов по метрикам.
# Скрипт работал по расписанию.
# После каждого цикла работы скрипта, результаты записывались в лог (отдельный текстовый файл)
# Перед каждым новым циклом скрипт читал лог, сравнивал лог с полученной статистикой, находил несоответствия и только после этого присылал алерт.


import requests
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import timedelta, datetime, date
import numpy as np
import io
import logging
import config


ch_engine = config.clickhouse_conn
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
    logger = logging.getLogger(name)
    if not logger.handlers:
        # Prevent logging from propagating to the root logger
        logger.propagate = 0
        console = logging.StreamHandler()
        logger.addHandler(console)
        formatter = logging.Formatter('%(asctime)s \t%(levelname)s \t%(name)s \t%(message)s')
        console.setFormatter(formatter)
    return logger


def get_query():
    get_q = f"""
                WITH 
                7 as days_for_diff,
                100 as min_shows_gam,
                (SELECT min(start_date) as start_date FROM ads.asian_holdings_and_sites FINAL) as min_date,
                holding_name as (
                    SELECT 
                        web_name, 
                        site_name, 
                        user_id, 
                        if(site_id != 0, site_id, user_id) as user_site
                    FROM ads.asian_holdings FINAL
                ),
                holdings_and_sites as (
                    select 
                        asian_holdings_and_sites.user_id as user_id,
                        if(site_id is null, user_site, site_id) as site_id,
                        asian_holdings_and_sites.user_site as user_site,
                        fixRPM,
                        type as traffic_type,
                        start_date,
                        limit,
                        if(end_date is not null, end_date, today()) as end_date,
                        if(web_name is null, web_name2, web_name) as holding, 
                        if(site_name is null, name, site_name) as site
                    from ads.asian_holdings_and_sites FINAL
                    LEFT JOIN (
                        SELECT 
                            web_name, 
                            site_name, 
                            user_id, 
                            if(site_id != 0, site_id, user_id) as user_site
                        FROM ads.asian_holdings FINAL
                    ) as names ON names.user_site = asian_holdings_and_sites.user_site
                    LEFT JOIN (
                        SELECT 
                            web_name as web_name2, 
                            site_name as site_name2, 
                            user_id
                        FROM ads.asian_holdings FINAL
                    ) names2 ON asian_holdings_and_sites.user_id = names2.user_id 
                    LEFT JOIN ads.sites s ON asian_holdings_and_sites.user_site = s.id
                    ),
                date_ranges as (
                    SELECT DISTINCT
                        date_ranges as date,
                        user_site
                    FROM (
                        SELECT 
                        date_ranges
                        FROM (
                        SELECT
                        arrayMap(d -> min_date + d, range(toUInt64(dateDiff('day', min_date, today()+1)))) AS date_ranges 
                        ) as date_ranges
                        ARRAY JOIN date_ranges
                    ) as dates 
                    ARRAY JOIN (SELECT groupArray(user_site) as user_id FROM ads.asian_holdings_and_sites ahas) as user_site
                ),
                dates_holdings_sites as (
                    select
                        date,
                        user_site,
                        user_id,
                        fixRPM,
                        traffic_type,
                        start_date,
                        end_date,
                        limit,
                        holding, 
                        site
                    from date_ranges
                    ASOF inner join holdings_and_sites
                    on date_ranges.user_site == holdings_and_sites.user_site and date_ranges.date >= holdings_and_sites.start_date 
                    where date between start_date and end_date
                ),
                gam_stats as (
                    SELECT 
                        date,
                        user_id,
                        site_id,
                        if(site_id is not null, site_id, user_id) as user_site,
                        gam_shows
                    FROM ads.gam_stats gs FINAL
                    WHERE gam_shows >= min_shows_gam
                ),
                stats as (
                    SELECT 
                        date,
                        webmaster_id,
                        if(site_id in (SELECT user_site FROM ads.asian_holdings_and_sites GROUP BY user_site), site_id, webmaster_id) as user_site,
                        sum(block_shows_count) as block_shows_count,
                        sum(confirmed_block_shows_count) as confirmed_block_shows_count,
                        sum(money_usd) as money_usd,
                        sum(clicks_count) as clicks_count 
                    FROM ads.v_daily_webmasters
                    WHERE date >= min_date and webmaster_id in (SELECT DISTINCT user_id FROM ads.asian_holdings_and_sites)
                    GROUP BY 
                        date, 
                        webmaster_id, 
                        user_site
                ),
                full_stats as (
                SELECT 
                    stats_with_date.date as date,
                    stats_with_date.user_id as user_id,
                    if(webmaster_id = 0, user_id, webmaster_id) as webmaster_id,
                    stats_with_date.user_site as user_site,
                    block_shows_count,
                    traffic_type,
                    confirmed_block_shows_count,
                    ifNull(money_usd, 0) as money_usd,
                    clicks_count,
                    fixRPM,
                    gam_shows,
                    holding, 
                    site,
                    start_date,
                    end_date,
                    limit
                FROM (
                    SELECT 
                        dates_holdings_sites.date as date,
                        dates_holdings_sites.user_site as user_site,
                        user_id,
                        traffic_type,
                        gam_shows,
                        fixRPM,
                        holding, 
                        site,
                        start_date,
                        end_date,
                        limit
                    FROM dates_holdings_sites
                    LEFT JOIN gam_stats on dates_holdings_sites.date = gam_stats.date and dates_holdings_sites.user_site = gam_stats.user_site 
                    ) as stats_with_date
                LEFT JOIN stats ON stats.date = stats_with_date.date and stats.user_site = stats_with_date.user_site
                ),
                avg_diff_gam_shows as (
                SELECT 
                    user_site,
                    (sum(gam_shows) - sum(block_shows_count) ) / sum(block_shows_count)  as shows_diff
                FROM full_stats
                LEFT JOIN (
                    SELECT 
                            user_site,
                            min(date) as first_gam_stats,
                            max(date) as last_gam_stats
                    FROM gam_stats gs 
                    WHERE gam_shows > 0
                    GROUP by user_site
                ) as min_max_date USING user_site 
                WHERE traffic_type = 'GAM' 
                    and dateDiff('day', date, last_gam_stats) < days_for_diff
                    and dateDiff('day', date, last_gam_stats) >= 0
                    and full_stats.gam_shows > 0
                GROUP BY user_site
                )
                SELECT
                    if(site='остальные', 'Other', site) as site,
                    holding,
                    user_site,
                    traffic_type,
                    start_date,
                    end_date,
                    dateDiff('day', start_date, end_date) as day_diff,
                    limit,
                    sum(block_shows_count) as block_shows_count,
                    sum(confirmed_block_shows_count) as confirmed_block_shows_count,
                    sum(money_usd) as money_usd,
                    sum(waiting_money) as waiting_money,
                    sum(clicks_count) as clicks_count,
                    sum(gam_shows) as gam_shows,
                    sum(gam_shows_pred) as gam_shows_pred
                from (
                SELECT *,
                    round(if(gam_shows > 0, gam_shows * 1.00, 
                            if(traffic_type='GAM', block_shows_count + (block_shows_count * shows_diff), 0)
                            )
                        ) as gam_shows_pred,
                    if(traffic_type='GAM', 
                        if(gam_shows > 0, 
                                gam_shows * fixRPM / 1000, 
                                gam_shows_pred * fixRPM / 1000
                                ), 
                        if(traffic_type='vRPM',
                            confirmed_block_shows_count * fixRPM / 1000,
                            block_shows_count * fixRPM / 1000)) as waiting_money
                FROM full_stats
                LEFT JOIN avg_diff_gam_shows USING user_site)
                where holding not in ('MPI') and limit>0
                group by 	
                    site,
                    holding,
                    user_site,
                    traffic_type,
                    start_date,
                    end_date,
                    day_diff,
                    limit
                """
    return get_q


def get_data(q, con):
    df = pd.read_sql(q, con)
    cnt_str = len(df)
    return df, cnt_str


def transform(df):
    df['shows_traff_type'] = 0
    df['shows_traff_type_convert'] = 0
    df['limit_convert'] = 0
    df['percent_of_shows'] = 0
    df['shows_daily'] = 0
    df['predict_cnt_days'] = 0
    df['roi'] = 0
    for i in df.index:
        if df.loc[i, 'traffic_type'] == 'GAM':
            df.loc[i, 'shows_traff_type'] = df.loc[i, 'gam_shows_pred']
            df.loc[i, 'shows_traff_type_convert'] = round(df.loc[i, 'shows_traff_type']/1000000, 1)
            df.loc[i, 'limit_convert'] = round(df.loc[i, 'limit']/1000000, 1)
            df.loc[i, 'percent_of_shows'] = df.loc[i, 'gam_shows_pred']/df.loc[i, 'limit']
            df.loc[i, 'shows_daily'] = df.loc[i, 'gam_shows_pred']//df.loc[i, 'day_diff']
            df.loc[i, 'predict_cnt_days'] = (df.loc[i, 'limit'] - df.loc[i, 'gam_shows_pred'])//df.loc[i, 'shows_daily']
            df.loc[i, 'roi'] = (df.loc[i, 'money_usd'] - df.loc[i, 'waiting_money'])/df.loc[i, 'waiting_money']
        elif df.loc[i, 'traffic_type'] == 'RPM':
            df.loc[i, 'shows_traff_type'] = df.loc[i, 'block_shows_count']
            df.loc[i, 'shows_traff_type_convert'] = round(df.loc[i, 'shows_traff_type']/1000000, 1)
            df.loc[i, 'limit_convert'] = round(df.loc[i, 'limit']/1000000, 1)
            df.loc[i, 'percent_of_shows'] = df.loc[i, 'block_shows_count']/df.loc[i, 'limit']
            df.loc[i, 'shows_daily'] = df.loc[i, 'block_shows_count']//df.loc[i, 'day_diff']
            df.loc[i, 'predict_cnt_days'] = (df.loc[i, 'limit'] - df.loc[i, 'block_shows_count'])//df.loc[i, 'shows_daily']
            df.loc[i, 'roi'] = (df.loc[i, 'money_usd'] - df.loc[i, 'waiting_money'])/df.loc[i, 'waiting_money']
        elif df.loc[i, 'traffic_type'] == 'vRPM':
            df.loc[i, 'shows_traff_type'] = df.loc[i, 'confirmed_block_shows_count']
            df.loc[i, 'shows_traff_type_convert'] = round(df.loc[i, 'shows_traff_type']/1000000, 1)
            df.loc[i, 'limit_convert'] = round(df.loc[i, 'limit']/1000000, 1)
            df.loc[i, 'percent_of_shows'] = df.loc[i, 'confirmed_block_shows_count']/df.loc[i, 'limit']
            df.loc[i, 'shows_daily'] = df.loc[i, 'confirmed_block_shows_count']//df.loc[i, 'day_diff']
            df.loc[i, 'predict_cnt_days'] = (df.loc[i, 'limit'] - df.loc[i, 'confirmed_block_shows_count'])//df.loc[i, 'shows_daily']
            df.loc[i, 'roi'] = (df.loc[i, 'money_usd'] - df.loc[i, 'waiting_money'])/df.loc[i, 'waiting_money']
    for i in df.index:
        if df.loc[i, 'predict_cnt_days'] < 0:
            df.loc[i, 'predict_cnt_days'] = 0
    return df



def create_tg_mes(df):
    m = ''
    for i in df.index:
        if df.loc[i, 'percent_window']== '50-74':
            m += '🇮🇩 '+f'<b><i>{df.loc[i, "holding"]}</i></b> ' + f'<b><i>{df.loc[i, "site"]}</i></b> '+ f'<b><i>({df.loc[i, "traffic_type"]})</i></b>' + '\n' \
            + f'{df.loc[i, "shows_traff_type_convert"]} млн. ' + f'из {df.loc[i, "limit_convert"]} млн. ' + f'(<b>{df.loc[i, "percent_of_shows"]} %</b>) ' + '\n'\
            + f'осталось ~ {df.loc[i, "predict_cnt_days"]} суток'  + '\n' \
            + f'<b>ROI: {df.loc[i, "roi"]} %</b>' + '\n' + '\n'
            logger.info(f'Нашел строки со значениями 50-74. Добавил их в сообщение')
            with open("/home/admin/scripts/percent_alert_log.txt", "a+") as file_object:
                file_object.seek(0)
                data = file_object.read(100)
                if len(data) > 0 :
                    file_object.write("\n")
                file_object.write(f'{df.loc[i, "site"]}' + '/' + f'{df.loc[i, "holding"]}'+ '/'+ f'{df.loc[i, "user_site"]}'+ '/'\
                +f'{df.loc[i, "start_date"]}'+ '/'+ f'{df.loc[i, "end_date"]}'+ '/' +f'{df.loc[i, "shows_traff_type"]}'+ '/' + f'{df.loc[i, "limit"]}'\
                + '/' + f'{df.loc[i, "percent_of_shows"]}' + '/' + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        elif df.loc[i, 'percent_window']== '75-89':
            m += '🇮🇩 '+f'<b><i>{df.loc[i, "holding"]}</i></b> ' + f'<b><i>{df.loc[i, "site"]}</i></b> '+ f'<b><i>({df.loc[i, "traffic_type"]})</i></b>' + '\n' \
            + f'{df.loc[i, "shows_traff_type_convert"]} млн. ' + f'из {df.loc[i, "limit_convert"]} млн. ' + f'(<b>{df.loc[i, "percent_of_shows"]} %</b>) ' + '\n'\
            + f'осталось ~ {df.loc[i, "predict_cnt_days"]} суток'  + '\n' \
            + f'<b>ROI: {df.loc[i, "roi"]} %</b>' + '\n' + '\n'
            logger.info(f'Нашел строки со значениями 75-89. Добавил их в сообщение')
            with open("/home/admin/scripts/percent_alert_log.txt", "a+") as file_object:
                file_object.seek(0)
                data = file_object.read(100)
                if len(data) > 0 :
                    file_object.write("\n")
                file_object.write(f'{df.loc[i, "site"]}' + '/' + f'{df.loc[i, "holding"]}'+ '/'+ f'{df.loc[i, "user_site"]}'+ '/'\
                +f'{df.loc[i, "start_date"]}'+ '/'+ f'{df.loc[i, "end_date"]}'+ '/' +f'{df.loc[i, "shows_traff_type"]}'+ '/' + f'{df.loc[i, "limit"]}'\
                + '/' + f'{df.loc[i, "percent_of_shows"]}' + '/' + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        elif df.loc[i, 'percent_window']== '90-100':
            m += '🇮🇩 '+f'<b><i>{df.loc[i, "holding"]}</i></b> ' + f'<b><i>{df.loc[i, "site"]}</i></b> '+ f'<b><i>({df.loc[i, "traffic_type"]})</i></b>' + '\n' \
            + f'{df.loc[i, "shows_traff_type_convert"]} млн. ' + f'из {df.loc[i, "limit_convert"]} млн. ' + f'(<b>{df.loc[i, "percent_of_shows"]} %</b>) ' + '\n'\
            + f'осталось ~ {df.loc[i, "predict_cnt_days"]} суток'  + '\n' \
            + f'<b>ROI: {df.loc[i, "roi"]} %</b>' + '\n' + '\n'
            logger.info(f'Нашел строки со значениями 90-100. Добавил их в сообщение')
            with open("/home/admin/scripts/percent_alert_log.txt", "a+") as file_object:
                file_object.seek(0)
                data = file_object.read(100)
                if len(data) > 0 :
                    file_object.write("\n")
                file_object.write(f'{df.loc[i, "site"]}' + '/' + f'{df.loc[i, "holding"]}'+ '/'+ f'{df.loc[i, "user_site"]}'+ '/'\
                +f'{df.loc[i, "start_date"]}'+ '/'+ f'{df.loc[i, "end_date"]}'+ '/' +f'{df.loc[i, "shows_traff_type"]}'+ '/' + f'{df.loc[i, "limit"]}'\
                + '/' + f'{df.loc[i, "percent_of_shows"]}' + '/' + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        elif df.loc[i, 'percent_window']== '100+':
            m += '🇮🇩 '+f'<b><i>{df.loc[i, "holding"]}</i></b> ' + f'<b><i>{df.loc[i, "site"]}</i></b> '+ f'<b><i>({df.loc[i, "traffic_type"]})</i></b>' + '\n' \
            + f'{df.loc[i, "shows_traff_type_convert"]} млн. ' + f'из {df.loc[i, "limit_convert"]} млн. ' + f'(<b>{df.loc[i, "percent_of_shows"]} %</b>) ' + '\n'\
            + f'осталось ~ {df.loc[i, "predict_cnt_days"]} суток'  + '\n' \
            + f'<b>ROI: {df.loc[i, "roi"]} %</b>' + '\n' + '\n'
            logger.info(f'Нашел строки со значениями 100+. Добавил их в сообщение')
            with open("/home/admin/scripts/percent_alert_log.txt", "a+") as file_object:
                file_object.seek(0)
                data = file_object.read(100)
                if len(data) > 0 :
                    file_object.write("\n")
                file_object.write(f'{df.loc[i, "site"]}' + '/' + f'{df.loc[i, "holding"]}'+ '/'+ f'{df.loc[i, "user_site"]}'+ '/'\
                +f'{df.loc[i, "start_date"]}'+ '/'+ f'{df.loc[i, "end_date"]}'+ '/' +f'{df.loc[i, "shows_traff_type"]}'+ '/' + f'{df.loc[i, "limit"]}'\
                + '/' + f'{df.loc[i, "percent_of_shows"]}' + '/' + datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    msg = (f'{m}')
    if len(m)>0:
        send_message(message=msg, chat_id=chat_id, token_id=telegram_token)
        logger.info(f'Отправил пороговый алерт')
    else:
        logger.info(f'Порогового алерта не будет! Никто не достиг порога')


def percent_alerting(df):
    df['percent_of_shows'] = round(df['percent_of_shows']*100, 2)
    df['roi'] = round(df['roi']*100, 2)
    df['percent_window'] = 0
    df['start_date'] = pd.to_datetime(df['start_date']).dt.strftime("%Y-%m-%d")
    for i in df.index:
        if df.loc[i, 'percent_of_shows'] < 50:
            df.loc[i, 'percent_window'] = '0-50'
        elif df.loc[i, 'percent_of_shows'] >=50 and df.loc[i, 'percent_of_shows'] < 75:
            df.loc[i, 'percent_window'] = '50-74'
        elif df.loc[i, 'percent_of_shows'] >=75 and df.loc[i, 'percent_of_shows'] < 90:
            df.loc[i, 'percent_window'] = '75-89'
        elif df.loc[i, 'percent_of_shows'] >=90 and df.loc[i, 'percent_of_shows'] <= 100:
            df.loc[i, 'percent_window'] = '90-100'
        elif df.loc[i, 'percent_of_shows'] >100:
            df.loc[i, 'percent_window'] = '100+'
    data_txt = pd.read_csv('/home/admin/scripts/percent_alert_log.txt',sep='/', header=None)
    data_txt.columns = ['site', 'holding', 'user_site', 'start_date', 'end_date', 'shows_traff_type', 'limit', 'percent', 'task_run']
    data_txt = data_txt[['site', 'holding', 'user_site', 'start_date', 'end_date', 'limit', 'percent']]
    data_txt['percent_window'] = 0
    for i in data_txt.index:
        if data_txt.loc[i, 'percent'] < 50:
            data_txt.loc[i, 'percent_window'] = '0-50'
        elif data_txt.loc[i, 'percent'] >=50 and data_txt.loc[i, 'percent'] < 75:
            data_txt.loc[i, 'percent_window'] = '50-74'
        elif data_txt.loc[i, 'percent'] >=75 and data_txt.loc[i, 'percent'] < 90:
            data_txt.loc[i, 'percent_window'] = '75-89'
        elif data_txt.loc[i, 'percent'] >=90 and data_txt.loc[i, 'percent'] <= 100:
            data_txt.loc[i, 'percent_window'] = '90-100'
        elif data_txt.loc[i, 'percent'] >100:
            data_txt.loc[i, 'percent_window'] = '100+'
    merg_df = df.merge(data_txt, how='left', on=['site', 'percent_window', 'user_site', 'start_date', 'limit'], indicator=True)
    merg_df = merg_df[merg_df['_merge']=='left_only']
    merg_df.rename(columns = {merg_df.columns[0]: "site", merg_df.columns[1]: "holding", merg_df.columns[2]: "user_site", merg_df.columns[4]: "start_date", 
    merg_df.columns[5]: "end_date", merg_df.columns[7]: "limit"}, inplace = True)
    create_tg_mes(merg_df)


def process(table_from, con_from):
    get_q = get_query()
    df, cnt_str = get_data(q=get_q,con=con_from)
    logger.info(f'Получил {cnt_str} строк из {table_from}')
    df_1 = transform(df)
    percent_alerting(df=df_1)


if __name__ == '__main__':
    logger = get_logger('asia_alert')
    logger.setLevel(logging.INFO)
    try:
        process(table_from='ads.asia', con_from=ch_engine)
    except Exception as e:
        logger.error(f'Task fail - {e}')
        msg = f'❌ Task fail! (asia_alert)'
        send_message(message=msg, chat_id=chat_id, token_id=telegram_token)
        raise