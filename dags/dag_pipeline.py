from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

from module.extract import download_full_load,upload_raw_data
from module.transform import cleansing_data,upload_clean_data,upload_aggregate_data
from module.load import load_aggregate,create_table_data,upload_to_mysql

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
        python_callable = upload_raw_data,
        op_kwargs = {"folder":"full_load"}
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
    
    upload_aggregate_data = PythonOperator(
        task_id = "upload_aggregate_data",
        python_callable = upload_aggregate_data,
        op_kwargs = {"stage" : "full_load"}
    )

    load_aggregate = PythonOperator(
        task_id = "load_aggregate_data",
        python_callable = load_aggregate,
        op_kwargs = {"folder":"full_load"}
    )
    
    create_table_data = PythonOperator(
        task_id = "create_table_data",
        python_callable = create_table_data,
    )
    
    upload_to_mysql = PythonOperator(
        task_id = "upload_to_mysql",
        python_callable = upload_to_mysql
    )

    
download_full_load >> upload_full_load >> cleansing_data >> upload_clean_data >> upload_aggregate_data

upload_aggregate_data >> load_aggregate >> create_table_data >> upload_to_mysql