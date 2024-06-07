import pandas as pd
from datetime import datetime
import os
import configparser
from io import StringIO
import boto3

def read_config():
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__),"config.ini"))
    access_key_id = config.get("S3", "access_key_id")
    secret_access_key = config.get("S3","secret_access_key")
    
    config_values = {
        "access_key_id": access_key_id,
        "secret_access_key": secret_access_key
    }
    return config_values

config_data = read_config()
access_key_id = config_data["access_key_id"]
secret_access_key = config_data["secret_access_key"]

session = boto3.Session(
    aws_access_key_id = access_key_id,
    aws_secret_access_key = secret_access_key
)

#Set Variable
s3_client = session.client('s3')
bucket_name = "project1forairflow"
str_datetime = str(datetime.now().strftime("%Y-%m-%d"))
customers_raw_file = f"rawfile/full_load/{str_datetime}_customers_rawdata.csv"
products_raw_file = f"rawfile/full_load/{str_datetime}_products_rawdata.csv"
sales_raw_file = f"rawfile/full_load/{str_datetime}_sales_rawdata.csv"

#Read file in S3 to Pandas DataFrame
customers_raw = s3_client.get_object(Bucket=bucket_name, Key=customers_raw_file)['Body'].read().decode('utf-8') 
products_raw = s3_client.get_object(Bucket=bucket_name, Key=products_raw_file)['Body'].read().decode('utf-8')
sales_raw = s3_client.get_object(Bucket=bucket_name, Key=sales_raw_file)['Body'].read().decode('utf-8')

customers_df = pd.read_csv(StringIO(customers_raw))
products_df = pd.read_csv(StringIO(products_raw))
sales_df = pd.read_csv(StringIO(sales_raw))

#Cleansing Customers
customers_df.set_index(["customer_id"],inplace=True)
type_customers = {"phone" : "object" , "zipcode" : "object"}
customers_df = customers_df.astype(type_customers)
format_phone = lambda x: x if pd.isna(x) else f"+66{str(x)}"
customers_df["phone"] = customers_df["phone"].apply(format_phone)

#Cleansing Products
products_df.set_index(["product_id"],inplace=True)

#Cleansing Sales
sales_df.set_index(["sale_id"],inplace=True)
sales_df["sale_date"] = pd.to_datetime(sales_df["sale_date"])
sales_df["sale_time"] = sales_df["sale_time"].str.slice(start = 7)

#Upload in S3
s3_resource = session.resource('s3')

s3_resource.Object(bucket_name, f"transition/{str_datetime}_customers.csv").put(Body=customers_df.to_csv(index=False))
