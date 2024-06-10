import pandas as pd
import os 
from datetime import datetime, timedelta

from module.function import connector,upload_file_s3,file_in_path

def download_full_load():
    mydb = connector("mysql")
    customers = pd.read_sql("SELECT * FROM customers",mydb).set_index(["customer_id"])
    products = pd.read_sql("SELECT * FROM products",mydb).set_index(["product_id"])
    sales = pd.read_sql("SELECT * FROM sales",mydb).set_index(["sale_id"])
    
    customers.to_csv(f"/opt/airflow/data/raw/customers_rawdata.csv")
    products.to_csv(f"/opt/airflow/data/raw/products_rawdata.csv")
    sales.to_csv(f"/opt/airflow/data/raw/sales_rawdata.csv")

def upload_full_load():
    str_datetime = str(datetime.now().strftime("%Y-%m-%d"))
    file_location = f"/opt/airflow/data/raw/"
    # files_name = [i for x in os.walk(file_location) for i in x[-1]]
    files_name = file_in_path(file_location)
    
    bucket_name = "project1forairflow"
    object_location = f"rawfile/full_load/{str_datetime}/"
    for file in files_name:
        file_name = f"{file_location}{file}"
        object_name = f"{object_location}{file}"
        try:
            upload_file_s3(file_name,bucket_name,object_name)
        except Exception as e:
            print(e)
        finally:
            if os.path.exists(file_name):
                os.remove(file_name)
                print(f"the file {file_name} has been delete.")
            else:
                print(f"the file {file_name} does not exist.")

        

