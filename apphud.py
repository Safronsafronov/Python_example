# Скрипт выполняе инсерт данных из одной таблицы в другую


import pandas as pd
from sqlalchemy import create_engine
import requests
from datetime import datetime
import logging
import config_lm

TOKEN = config_lm.baton_bot_token
CHAT_ID = config_lm.lm_tg_chat


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


def send_message(message, chat_id=CHAT_ID, token_id=TOKEN):
    # Функция отвечающая за отправку сообщения телеграм ботом
    r = requests.post(f'https://api.telegram.org/bot{token_id}/sendMessage',
                      data={
                        'chat_id': chat_id,
                        'parse_mode': 'html',
                        'text': message},
                      timeout=100)
    return r.status_code


def fail_message():
    now = datetime.now().strftime("<b> %d/%m/%Y %H:%M:%S </b>")
    logger.info(now)  # проверка отметки времени в терминале
    msg = ' ❌ ❌ ❌ Task fail! (user features) ' + now
    logger.info(msg)  # проверка сообщения в терминале
    send_message(msg)  # отправка сообщения


def get_query():
    # Получение запроса
    sql = '''insert into agg.apphud_events(
            appsflyer_id, install_time, event_revenue_usd, event_time, event_hash, event_name, app_id, app_name, 
            country_code, media_source, campaign, af_channel, af_content_id, event_type, subscription_duration, 
            upload_time)
            select
            appsflyer_id,
            install_time,
            event_revenue_usd,
            event_time,
            event_hash,
            event_name,
            app_id,
            app_name,
            country_code,
            media_source,
            campaign,
            af_channel,
            af_content_id,  
            if(event_name in ('apphud_trial_converted','apphud_subscription_started','apphud_intro_started'),'sbscr_on',
            if(event_name in ('apphud_subscription_canceled','apphud_intro_canceled','apphud_autorenew_disabled'),'sbscr_off',
            if(event_name = 'install', 'install', 
            if(event_name in ('apphud_trial_expired', 'apphud_trial_canceled'), 'trial_off',
            if (event_name = 'apphud_subscription_renewed', 'sbscr_renew',
            if(event_name in ('apphud_trial_started'), 'trial_on', 
            if(event_name in ('apphud_intro_refunded', 'apphud_subscription_refunded'), 'refund',
            if(event_name in ('apphud_subscription_expired'), 'expiration', NULL)))))))) as event_type,
            if(like(af_content_id, '%%year%%'), 365, 
            if(like(af_content_id, '%%.week%%'), 7, 
            if(like(af_content_id, '%%.one.month%%'), 30, 
            if(like(af_content_id, '%%.six.months%%') OR like(af_content_id, '%%.sixmonths%%'), 180,
            if(like(af_content_id, '%%.threemonths%%'), 90, NULL))))) as subscription_duration,
            now() as upload_time
            from raw.appsflyer_raw_data_parsed
            where like(event_name, '%%apphud%%')
            and length(appsflyer_id) > 0
            and upload_time >= (select max(upload_time)-60 from agg.apphud_events)'''
    return sql


def optimize_data(conn):
    # Оптимизация помесячной партиции таблички
    opt_sql = ''' OPTIMIZE TABLE agg.apphud_events FINAL DEDUPLICATE'''
    optimize = pd.read_sql(opt_sql, conn)
    logger.info(f'Оптимизация данных.')
    return optimize


def load_data(conn):
    # Выполнение sql запроса
    q = get_query()
    pd.read_sql(q, conn)
    logger.info(f'Данные загружены.')


def operation():
    logger.info('Обновление данных таблички agg.apphud_events:')
    # Подключение к указаной в engine датабазе
    conn = create_engine(config_lm.ch_connect)
    logger.info('Подключение к датабазе успешно')
    load_data(conn)
    optimize_data(conn)
    logger.info('Работа с табличкой закончена')


if __name__ == '__main__':  # запуск
    logger = get_logger('apphud_events')
    logger.setLevel(logging.INFO)
    try:
        operation()
    except Exception as e:
        logger.error(f'Task fail - {e}')
        fail_message()
        raise
