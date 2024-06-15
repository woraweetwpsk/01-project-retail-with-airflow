from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

from module.input_daily_data import _input_daily_data

with DAG(
    "input_daily_data",
    description="daily input data",
    start_date=timezone.datetime(2024,6,10),
    schedule_interval="30 5 * * *",
    catchup=False,
    tags=["retail"]
):
    
    input_daily_data = PythonOperator(
        task_id = "input_daily_data",
        python_callable = _input_daily_data
    )
    
input_daily_data