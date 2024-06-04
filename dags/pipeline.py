from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from module.extract import upload_full_load,download_full_load
# from module.transform import
# from module.load import

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
    
download_full_load >> upload_full_load
