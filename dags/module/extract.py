import pandas as pd
import mysql.connector
import os
import configparser
from datetime import datetime, timedelta
import logging
import boto3
from botocore.exceptions import ClientError

def read_config():
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__),"config.ini"))
    db_host = config.get("db", "host")
    db_user = config.get("db", "user")
    db_password = config.get("db", "password")
    db_name = config.get("db","name")
    access_key_id = config.get("S3", "access_key_id")
    secret_access_key = config.get("S3","secret_access_key")
    
    config_values = {
        "db_host": db_host,
        "db_user": db_user,
        "db_password": db_password,
        "db_name": db_name,
        "access_key_id": access_key_id,
        "secret_access_key": secret_access_key
    }
    return config_values


def timedelta_to_time(td):
    return (datetime.min + td).time()

def download_full_load():
    config_data=read_config()
    db_host = config_data["db_host"]
    db_user = config_data["db_user"]
    db_password = config_data["db_password"]
    db_name = config_data["db_name"]
    
    mydb = mysql.connector.connect(
    host = db_host,
    user = db_user,
    password = db_password,
    database = db_name
    )

    # with mydb.cursor() as mycursor:
    with mydb.cursor() as mycursor:
    # mycursor = mydb.cursor()
    #Download all data
        customers = pd.read_sql("SELECT * FROM customers",mydb).set_index(["customer_id"])
        products = pd.read_sql("SELECT * FROM products",mydb).set_index(["product_id"])
        sales = pd.read_sql("SELECT * FROM sales",mydb).set_index(["sale_id"])

    #change sale_date type Object to datetime
    sales["sale_date"] = pd.to_datetime(sales["sale_date"])
    #chage sale_time type deltatime to Object
    sales["sale_time"] = sales["sale_time"].apply(timedelta_to_time)
    
    #download to local file path
    customers.to_csv(f"/opt/airflow/data/customers_rawdata.csv")
    products.to_csv(f"/opt/airflow/data/products_rawdata.csv")
    sales.to_csv(f"/opt/airflow/data/sales_rawdata.csv")
    
def upload_full_load():
    config_data = read_config()
    access_key_id = config_data["access_key_id"]
    secret_access_key = config_data["secret_access_key"]
    
    file_name = ["customers_rawdata.csv","products_rawdata.csv","sales_rawdata.csv"]
    
    for i in file_name:
        logging.basicConfig(level=logging.INFO)
        logger = logging.getLogger()
        bucket_name = "project1forairflow"
        s3_file_key = f"rawfile/{i}"
        file_path = f"/opt/airflow/data/{i}"
        s3 = boto3.client('s3',aws_access_key_id= access_key_id,aws_secret_access_key= secret_access_key)
        
        try:
            s3.upload_file(file_path, bucket_name, s3_file_key)
            logger.info(f'File {file_path} upload to {bucket_name}/{s3_file_key}')
        except Exception as e:
            logger.error(e)
            

    
    


