from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pendulum
import requests

#Weather
def hw_7_get_temp(**kwargs):

    ti = kwargs['ti']

    city = "Vladivostok"

    api_key = "7f1baa1d74ab9d393b97eab4b600ef44"

    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"

    payload = {}

    headers = {}

    response = requests.request("GET", url, headers=headers, data=payload)

    # ti.xcom_push(key='hw_7_open_weather', value=round(float(response.json()['main']['temp'])-273.15, 2)

    return round(float(response.json()['main']['temp'])-273.15, 2)


def hw_7_check_temp(ti):

    temp = int(ti.xcom_pull(task_ids='hw_7_get_temperature_task'))

    print(f'Temperature now is {temp}')

    if temp >= 15:

        return 'hw_7_print_warm'

    else:

        return 'hw_7_print_cold'


with DAG(

        'hw_7_weather_check_warm_or_cold',

        start_date=datetime(2024, 1, 1),

        catchup=False,

        tags=['homework_ETL'],

) as dag:

    hw_7_get_temperature_task = PythonOperator(

        task_id='hw_7_get_temperature_task',

        python_callable=hw_7_get_temp,

    )

    hw_7_check_temperature_task = BranchPythonOperator(

        task_id='hw_7_check_temperature_task',

        python_callable=hw_7_check_temp,

    )

    hw_7_print_warm = BashOperator(

        task_id='hw_7_print_warm',

        bash_command='echo "It is warm"',

    )

    hw_7_print_cold = BashOperator(

        task_id='hw_7_print_cold',

        bash_command='echo "It is cold"',

    )

    hw_7_get_temperature_task >> hw_7_check_temperature_task >> [
        hw_7_print_warm, hw_7_print_cold]
        
