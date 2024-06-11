from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

from module.extract import download_daily_data,upload_daily_data
from module.transform_daily import cleansing_daily_data,upload_clean_daily,aggregate_daily,upload_aggregate_daily

with DAG(
    "daily_pipeline",
    start_date=timezone.datetime(2024,5,30),
    schedule=None,
    tags=["retail"]
):
    download_daily_data = PythonOperator(
        task_id = "download_daily_data",
        python_callable = download_daily_data
    )
    
    upload_daily_data = PythonOperator(
        task_id = "upload_daily_data",
        python_callable = upload_daily_data
    )
    
    cleansing_daily_data = PythonOperator(
        task_id = "cleansing_daily_data",
        python_callable = cleansing_daily_data
    )
    
    upload_clean_daily = PythonOperator(
        task_id = "upload_clean_daily",
        python_callable = upload_clean_daily
    )
    
    aggregate_daily = PythonOperator(
        task_id = "aggregate_daily",
        python_callable = aggregate_daily
    )
    
    upload_aggregate_daily = PythonOperator(
        task_id = "upload_aggregate_daily",
        python_callable = upload_aggregate_daily
    )
    
download_daily_data >> upload_daily_data >> cleansing_daily_data >> upload_clean_daily >> aggregate_daily >> upload_aggregate_daily