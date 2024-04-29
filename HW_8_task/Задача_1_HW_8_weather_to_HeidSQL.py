import pandas as pd
from sqlalchemy import inspect, create_engine, Table, Column, Float, TIMESTAMP, MetaData, String
from datetime import datetime, timedelta
from pandas.io import sql
import pendulum
import requests

     # Функция для получения температуры и даты со временем из API OpenWeatherMap
def get_openweather_data(api_key, city):
    url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}' 
    response = requests.get(url)
    data = response.json()
    temperature = data['main']['temp']
    timestamp = data['dt']
    date_time = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    return round(float(temperature) - 273.15, 2), date_time    


    # Функция для получения температуры и даты со временем из API Yandex
def get_yandex_weather_data(api_key, lat, lon):
    url = f"https://api.weather.yandex.ru/v2/informers?lat={lat}&lon={lon}"
    headers = {'X-Yandex-API-Key': api_key}
    response = requests.get(url, headers=headers)
    data = response.json()
    
    try:
        temperature = data['fact']['temp']
        timestamp = data['now']
        date_time = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        return temperature, date_time
    except KeyError as e:
        print(f"KeyError: {e}")
        return None, None

   # API ключи и координаты для запросов
openweather_api_key = '2cd78e55c423fc81cebc1487134a6300'
yandex_weather_api_key = '33f45b91-bcd4-46e4-adc2-33cfdbbdd88e'
city = 'Moscow'
lat = '55.75396'
lon = '37.620393'

openweather_temperature, openweather_time = get_openweather_data(openweather_api_key, city)
yandex_temperature, yandex_time = get_yandex_weather_data(yandex_weather_api_key, lat, lon)


    # Подключение к БД, создание таблицы, получение данных и сохранение их в базу данных    
con = create_engine("mysql://Airflow:1@localhost/spark")
metadata = MetaData()

weather_table = Table(
    'Weather_Moscow',
    metadata,
    Column('source', String(50), nullable=True),
    Column('temperature', Float, nullable=True),
    Column('date_time', TIMESTAMP, nullable=True)
)

metadata.create_all(con)

ins = weather_table.insert()
values = [
    {"source": "OpenWeatherMap", "temperature": openweather_temperature, "date_time": openweather_time},
    {"source": "Yandex.Weather", "temperature": yandex_temperature, "date_time": yandex_time}
]

with con.connect() as connection:
    connection.execute(ins, values)
















