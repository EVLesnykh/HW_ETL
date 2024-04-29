import os
from datetime import timedelta
import pendulum
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.sql import select
from airflow.decorators import dag, task
from airflow.providers.telegram.operators.telegram import TelegramOperator

os.environ["no_proxy"] = "*"

def truncate(formatted_text, limit):
    if len(formatted_text) <= limit:
        return formatted_text
    else:
        return formatted_text[:limit]

@dag(
    dag_id="HW_8_Payments",
    schedule="@once",
    start_date=pendulum.datetime(2024, 4, 29, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)
def PaymentsETL():
    
    send_message_telegram_task = TelegramOperator(
        task_id='send_message_telegram',
        telegram_conn_id='telegram_default',
        token='6739587766:AAEyp8y2-LzFLASv7Z6FZPXb1riR15vKsTA',
        chat_id='1420330277',
        text=truncate('Payment data: {{ ti.xcom_pull(task_ids=["heidsql_data"], key="payments") }}', 4096)  # Ограничение на 4096 символов в telegram
    )
        
    @task(task_id='heidsql_data')
    def get_PaymentsETL(**kwargs):
        ti = kwargs['ti']
        con = create_engine("mysql://Airflow:1@localhost/spark")

        with con.connect() as connection:
            metadata = MetaData()
            payments_table = Table("Payments", metadata, autoload=True, autoload_with=con)    
            query = select([payments_table])
            result = connection.execute(query)
            payment_data = result.fetchall()
            
            formatted_text = "Данные о платежах:\n"
        
            for payment in payment_data:
                formatted_text += f"№: {payment['№']}, Month: {payment['Month']}, Payment amount: {round(payment['Payment amount'], 2)}, " \
                      f"Payment of the principal debt: {round(payment['Payment of the principal debt'], 2)}, " \
                      f"Payment of interest: {round(payment['Payment of interest'], 2)}, " \
                      f"Balance of debt: {round(payment['Balance of debt'], 2)}, Debt: {round(payment['Debt'], 2)}, Interest: {round(payment['Interest'], 2)}\n"
                    
        ti.xcom_push(key='payments', value=formatted_text)  # Сохраняем данные в XCom

    @task(task_id='python_payments')
    def get_Payments(**kwargs):
        print("PaymentsETL")        
      
    get_PaymentsETL() >> get_Payments() >> send_message_telegram_task

dag = PaymentsETL()
