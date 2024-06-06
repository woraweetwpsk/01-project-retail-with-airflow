from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from module.create_data_mysql import create_table,create_customers_data,create_products_data,create_sales_data


with DAG(
    "create_simple_data",
    start_date=timezone.datetime(2024,5,30),
    schedule=None,
    tags=["retail"]
):
    
    create_table = PythonOperator(
        task_id = "create_table",
        python_callable = create_table
    )
    
    create_customers_data = PythonOperator(
        task_id = "create_customer_data",
        python_callable = create_customers_data
    )
    
    create_products_data = PythonOperator(
        task_id = "create_products_data",
        python_callable = create_products_data
    )
    
    create_sales_data = PythonOperator(
        task_id = "create_sales_data",
        python_callable = create_sales_data
    )
    
create_table >> create_customers_data >> create_products_data >> create_sales_data