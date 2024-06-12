from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from module.extract import download_full_load,upload_full_load
from dags.module.transform import cleansing_data,upload_clean_data,func_agg_data,func_upload_agg

with DAG(
    "project_retail_full_load",
    start_date=timezone.datetime(2024,5,30),
    schedule=None,
    tags=["retail"]
):
    
    
    download_full_load = PythonOperator(
        task_id = "download_full_load",
        python_callable = download_full_load
    )

    upload_full_load = PythonOperator(
        task_id = "upload_full_load",
        python_callable = upload_full_load
    )
    
    cleansing_data = PythonOperator(
        task_id = "cleansing_data",
        python_callable = cleansing_data
    )
    
    upload_clean_data = PythonOperator(
        task_id = "upload_clean_data",
        python_callable = upload_clean_data,
        op_kwargs = {"stage" : "full_load"}
    )
    
    aggregate_data = PythonOperator(
        task_id = "aggregate_data",
        python_callable = func_agg_data,
        op_kwargs = {"stage" : "full_load"}
    )
    
    upload_aggregate_data = PythonOperator(
        task_id = "upload_aggregate_data",
        python_callable = func_upload_agg,
        op_kwargs = {"stage" : "full_load"}
    )

    
download_full_load >> upload_full_load >> cleansing_data >> upload_clean_data >>aggregate_data >> upload_aggregate_data
