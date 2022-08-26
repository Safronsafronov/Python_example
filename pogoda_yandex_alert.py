# Скрипт использовался для получения погоды по API яндекса
# Информация приходила каждое утро и вечер в Телеграм

import requests
import pandas as pd
from datetime import timedelta, datetime, date
import numpy as np
import io
import json
import logging
import config
# from geopy import geocoders


telegram_token = config.baton_bot_token
chat_id = config.tg_chat
city = 'Санкт-Петербург'
token_yandex = config.token_yandex


def send_message(message, chat_id=chat_id, token_id=telegram_token):
    # Функция отвечающая за отправку сообщения телеграм ботом
    r = requests.post(f'https://api.telegram.org/bot{token_id}/sendMessage',
                      data={
                          'chat_id': chat_id,
                          'parse_mode': 'html',
                          'text': message},
                      timeout=100)
    return r.status_code


def success_message(msg, city):
    now = datetime.now().strftime("<b> %d/%m/%Y %H:%M:%S </b>")
    logger.info(now)
    logger.info(msg) 
    send_message(msg)  


def fail_message():
    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    logger.info(now) 
    msg = 'Я хз, но при получении погоды что-то пошло не так :('
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


def geo_pos(city: str):
    geolocator = geocoders.Nominatim(user_agent="telebot")
    latitude = str(geolocator.geocode(city).latitude)
    longitude = str(geolocator.geocode(city).longitude)
    return latitude, longitude


def get_data(latitude, longitude, token_yandex):
    url_yandex = f'https://api.weather.yandex.ru/v2/forecast/?lat={latitude}&lon={longitude}'
    yandex_req = requests.get(url_yandex, headers={'X-Yandex-API-Key': token_yandex})
    yandex_json = json.loads(yandex_req.text)
    return yandex_json


def yandex_weather (latitude, longitude, token_yandex):
    yandex_json = get_data(latitude, longitude, token_yandex)
    conditions = {'clear': 'ясно', 'partly-cloudy': 'малооблачно', 'cloudy': 'облачно с прояснениями',
                  'overcast': 'пасмурно', 'drizzle': 'морось', 'light-rain': 'небольшой дождь',
                  'rain': 'дождь', 'moderate-rain': 'умеренно сильный', 'heavy-rain': 'сильный дождь',
                  'continuous-heavy-rain': 'длительный сильный дождь', 'showers': 'ливень',
                  'wet-snow': 'дождь со снегом', 'light-snow': 'небольшой снег', 'snow': 'снег',
                  'snow-showers': 'снегопад', 'hail': 'град', 'thunderstorm': 'гроза',
                  'thunderstorm-with-rain': 'дождь с грозой', 'thunderstorm-with-hail': 'гроза с градом'
                  }
    wind_dir = {'nw': 'северо-западное', 'n': 'северное', 'ne': 'северо-восточное', 'e': 'восточное',
                'se': 'юго-восточное', 's': 'южное', 'sw': 'юго-западное', 'w': 'западное', 'с': 'штиль'}
    yandex_json['fact']['condition'] = conditions[yandex_json['fact']['condition']]
    yandex_json['fact']['wind_dir'] = wind_dir[yandex_json['fact']['wind_dir']]
    pogoda = dict()
    params = ['temp','condition', 'wind_dir', 'wind_speed', 'pressure_mm', 'feels_like']
    pogoda['fact'] = dict()
    pogoda['yesterday'] = dict()
    pogoda['info'] = dict()
    pogoda['forecasts'] = dict()
    for i in params:
        pogoda['fact'][i] = yandex_json['fact'][i]
    pogoda['yesterday'] = yandex_json['yesterday']
    pogoda['info']['url'] = yandex_json['info']['url']
    pogoda['forecasts']['next_night'] = yandex_json['forecasts'][1]['parts']['night']['temp_avg']
    pogoda['forecasts']['next_day'] = yandex_json['forecasts'][1]['parts']['day']['temp_avg']
    pogoda['fact']['температура'] = pogoda['fact'].pop('temp')
    pogoda['fact']['небо'] = pogoda['fact'].pop('condition')
    pogoda['fact']['направление ветра'] = pogoda['fact'].pop('wind_dir')
    pogoda['fact']['скорость ветра'] = pogoda['fact'].pop('wind_speed')
    pogoda['fact']['давление'] = pogoda['fact'].pop('pressure_mm')
    pogoda['fact']['ощущается как'] = pogoda['fact'].pop('feels_like')
    pogoda['yesterday']['вчера в это время'] = pogoda['yesterday'].pop('temp')
    pogoda['forecasts']['завтра ночью'] = pogoda['forecasts'].pop('next_night')
    pogoda['forecasts']['завтра днем'] = pogoda['forecasts'].pop('next_day')
    pogoda['info']['более полная информация'] = pogoda['info'].pop('url')
    return pogoda


def print_yandex_weather(dict_weather_yandex, city):
    msg = (
    f'По данным Яндекса в городе {city}:'+'\n'
    f'Температура сейчас: ' + f'<b>{dict_weather_yandex["fact"]["температура"]}°</b>'+','+'\n'
    f'Ощущается как: ' + f'<b>{dict_weather_yandex["fact"]["ощущается как"]}°</b>'+','+'\n'
    f'Вчера в это время: ' + f'<b>{dict_weather_yandex["yesterday"]["вчера в это время"]}°</b>'+','+'\n'
    f'На небе: ' +  f'<b>{dict_weather_yandex["fact"]["небо"]}</b>'+','+'\n'
    f'Направление ветра: ' +  f'<b>{dict_weather_yandex["fact"]["направление ветра"]}</b>'+','+'\n'
    f'Скорость ветра: ' + f'<b>{dict_weather_yandex["fact"]["скорость ветра"]} м/с</b>'+','+'\n'
    f'Завтра ночью: ' + f'<b>{dict_weather_yandex["forecasts"]["завтра ночью"]}°</b>'+','+'\n'
    f'Завтра днем: ' +  f'<b>{dict_weather_yandex["forecasts"]["завтра днем"]}°</b>'+','+'\n'
    ' '+'\n'
    f' А здесь ссылка на подробности \n' + f'{dict_weather_yandex["info"]["более полная информация"]}')
    return msg


def big_weather(city, token_yandex):
    logger.info(f'Определяю широту и долготу для города {city}')
    # latitude, longitude = geo_pos(city)
    latitude = '59.917857350000006'
    longitude = '30.380619357025516'
    logger.info(f'Начинаю получать данные по API')
    df = yandex_weather(latitude, longitude, token_yandex)
    logger.info(f'Получил данные и сформировал словарь для города {city}')
    msg = print_yandex_weather(dict_weather_yandex=df, city=city)
    logger.info(f'Сформировал сообщение для города {city}')
    return msg


if __name__ == '__main__':
    logger = get_logger('pogoda')
    logger.setLevel(logging.INFO)
    try:
        msg = big_weather(city = city, token_yandex = token_yandex)
        success_message(msg=msg, city=city)
    except Exception as e:
        logger.error(f'Task fail - {e}')
        fail_message()
        raise