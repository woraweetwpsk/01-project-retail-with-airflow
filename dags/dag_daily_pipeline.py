from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

from module.extract import download_daily_data,upload_daily_data
from module.transform import cleansing_daily_data,upload_clean_data,func_agg_data,func_upload_agg


with DAG(
    "daily_pipeline",
    start_date=timezone.datetime(2024,5,30),
    schedule_interval="00 8 * * *",
    catchup=False,
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
    
    upload_clean_data = PythonOperator(
        task_id = "upload_clean_daily",
        python_callable = upload_clean_data,
        op_kwargs={"stage":"daily"}
    )
    
    daily_aggregate_data = PythonOperator(
        task_id = "daily_aggregate_data",
        python_callable = func_agg_data,
        op_kwargs={"stage":"daily"}
    )
    
    daily_upload_aggragate_data = PythonOperator(
        task_id = "daily_upload_aggregate_data",
        python_callable = func_upload_agg,
        op_kwargs={"stage":"daily"}
    )
    
    
download_daily_data >> upload_daily_data >> cleansing_daily_data >> upload_clean_data >> daily_aggregate_data >> daily_upload_aggragate_data