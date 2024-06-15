from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

from module.extract import download_daily_data,upload_raw_data
from module.transform import cleansing_daily_data,upload_clean_data,upload_aggregate_data
from module.load import load_aggregate,upload_to_mysql


with DAG(
    "daily_pipeline",
    start_date=timezone.datetime(2024,5,30),
    schedule_interval="00 6 * * *",
    catchup=False,
    tags=["retail"]
):
    
    download_daily_data = PythonOperator(
        task_id = "download_daily_data",
        python_callable = download_daily_data
    )
    
    upload_daily_data = PythonOperator(
        task_id = "upload_daily_data",
        python_callable = upload_raw_data,
        op_kwargs = {"folder":"daily"}
    )
    
    cleansing_daily_data = PythonOperator(
        task_id = "cleansing_daily_data",
        python_callable = cleansing_daily_data
    )
    
    upload_clean_data = PythonOperator(
        task_id = "upload_clean_daily",
        python_callable = upload_clean_data,
        op_kwargs={"stage":"daily"}
    )
    
    daily_upload_aggragate_data = PythonOperator(
        task_id = "daily_upload_aggregate_data",
        python_callable = upload_aggregate_data,
        op_kwargs={"stage":"daily"}
    )
    
    load_aggregate = PythonOperator(
        task_id = "daily_load_aggregate_data",
        python_callable = load_aggregate,
        op_kwargs={"folder":"daily"}
    )
    
    upload_to_mysql = PythonOperator(
        task_id = "daily_upload_to_mysql",
        python_callable = upload_to_mysql
    )
    
    
download_daily_data >> upload_daily_data >> cleansing_daily_data >> upload_clean_data >> daily_upload_aggragate_data
daily_upload_aggragate_data >> load_aggregate >> upload_to_mysql