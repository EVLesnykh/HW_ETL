import pandas as pd
from sqlalchemy import inspect, create_engine
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
from pandas.io import sql
import pendulum
import requests

# Функция для получения температуры и даты со временем из API OpenWeatherMap
def get_weather(api_key, city):
    url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}' 
    response = requests.get(url)
    data = response.json()
    temperature = data['main']['temp']
    timestamp = data['dt']
    date_time = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
    return round(float(temperature) - 273.15, 2), date_time
  
    
# Функция для записи температуры и текущего времени в базу данных
def save_weather_to_db(api_key, city):
    temperature, date_time = get_weather(api_key, city)
    
    con = create_engine("mysql://Airflow:1@localhost/spark")
    sql.execute("""drop table if exists spark.`Temperature_Vladivostok`""",con)
    sql.execute("""CREATE TABLE if not exists spark.`Temperature_Vladivostok` (
  `date_time` TIMESTAMP NULL DEFAULT NULL,
  `temperature` FLOAT NULL DEFAULT NULL
)
COLLATE='utf8mb4_general_ci'
ENGINE=InnoDB""",con)
     
    with con.connect() as connection:
        ins = f"INSERT INTO spark.`Temperature_Vladivostok` (date_time, temperature) VALUES ('{date_time}', {temperature})"
        connection.execute(ins)

# Получаем API ключ OpenWeatherMap
api_key = '7f1baa1d74ab9d393b97eab4b600ef44'
city = 'Vladivostok'

# Получаем данные погоды и записываем ее в базу данных
save_weather_to_db(api_key, city)
