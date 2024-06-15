import pandas as pd
import os 
import datetime

from module.function import connector,upload_file_s3,file_in_path

def download_full_load():
    mydb = connector("mysql")
    
    #Read sql and convert to Pandas Dataframe
    customers = pd.read_sql("SELECT * FROM customers",mydb).set_index(["customer_id"])
    products = pd.read_sql("SELECT * FROM products",mydb).set_index(["product_id"])
    sales = pd.read_sql("SELECT * FROM sales",mydb).set_index(["sale_id"])
    
    #Save file in path
    customers.to_csv(f"/opt/airflow/data/raw/customers_rawdata.csv")
    products.to_csv(f"/opt/airflow/data/raw/products_rawdata.csv")
    sales.to_csv(f"/opt/airflow/data/raw/sales_rawdata.csv")


def upload_raw_data(folder):
    str_datetime = str(datetime.datetime.now().strftime("%Y-%m-%d"))
    file_location = f"/opt/airflow/data/raw/"
    #Search and store file names in list format 
    files_name = file_in_path(file_location)
    
    bucket_name = "raw-file-us-east-1"
    
    #Loop upload all file in path
    for file in files_name:
        file_name = f"{file_location}{file}"
        object_name = f"{folder}/{str_datetime}/{file}"
        try:
            upload_file_s3(file_name,bucket_name,object_name)
        except Exception as e:
            print(e)


def download_daily_data():
    date = str(datetime.date.today() - datetime.timedelta(days = 0))
    mydb = connector("mysql")
    daily_sale = pd.read_sql(f"SELECT * FROM sales WHERE sale_date = '{date}'",mydb).set_index(["sale_id"])
    
    daily_sale.to_csv(f"/opt/airflow/data/raw/daily_sale.csv")