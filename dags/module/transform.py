import pandas as pd
import os
from datetime import datetime

from module.function import read_csv_s3,upload_file_s3,file_in_path,file_in_s3,search_new_path

    
def cleansing_data():
    bucket_name = "raw-file-us-east-1"
    str_datetime = str(datetime.now().strftime("%Y-%m-%d"))
    customers_df = read_csv_s3(bucket_name,f"full_load/{str_datetime}/customers_rawdata.csv")
    products_df = read_csv_s3(bucket_name,f"full_load/{str_datetime}/products_rawdata.csv")
    sales_df = read_csv_s3(bucket_name,f"full_load/{str_datetime}/sales_rawdata.csv")
    
    #Cleansing Customers Data
    customers_df.set_index(["customer_id"])
    type_customers = {"phone" : "object" , "zipcode" : "object"}
    customers_df = customers_df.astype(type_customers)
    format_phone = lambda x: x if pd.isna(x) else f"+66{str(x)}"
    customers_df["phone"] = customers_df["phone"].apply(format_phone)
    
    #Cleansing Products Data
    products_df.set_index(["product_id"])
    
    #Cleansing Sales Data
    sales_df.set_index(["sale_id"])
    sales_df["sale_date"] = pd.to_datetime(sales_df["sale_date"])
    sales_df["sale_time"] = sales_df["sale_time"].str.slice(start = 7)
    
    #Merge all data
    combine_all_df = pd.merge(pd.merge(sales_df,products_df,how="left",on="product_id"),customers_df,how="left",on="customer_id")
    
    #Save file in path
    path_transform = "/opt/airflow/data/transform/"
    customers_df.to_csv(f"{path_transform}customers_transform_data.csv",index=False)
    products_df.to_csv(f"{path_transform}products_transform_data.csv",index=False)
    sales_df.to_csv(f"{path_transform}sales_transform_data.csv",index=False)
    
    path_agg = "/opt/airflow/data/aggregate/"
    combine_all_df.to_csv(f"{path_agg}combine_all_data.csv",index=False)


def cleansing_daily_data():
    str_datetime = str(datetime.now().strftime("%Y-%m-%d"))
    bucket_raw_name = "raw-file-us-east-1"
    bucket_transform_name = "transfrom-us-east-1"
    lasted_path_full_load = search_new_path(bucket_transform_name,"full_load/")
    path_daily = f"daily/{str_datetime}/"
    
    #Read file to Pandas Dataframe
    sales_daily = read_csv_s3(bucket_raw_name,f"{path_daily}daily_sale.csv")
    customers_transform = read_csv_s3(bucket_transform_name,f"{lasted_path_full_load}/customers_transform_data.csv")
    products_transform = read_csv_s3(bucket_transform_name,f"{lasted_path_full_load}/products_transform_data.csv")
    
    #Cleansing Sales Data
    sales_daily.set_index(["sale_id"])
    sales_daily["sale_date"] = pd.to_datetime(sales_daily["sale_date"])
    sales_daily["sale_time"] = sales_daily["sale_time"].str.slice(start = 7)
    
    #Merge all data
    combine_all_df = pd.merge(pd.merge(sales_daily,products_transform,how="left",on="product_id"),customers_transform,how="left",on="customer_id")
    
    #Save file in path
    path = "/opt/airflow/data/"
    sales_daily.to_csv(f"{path}transform/transform_daily_sale.csv",index=False)
    combine_all_df.to_csv(f"{path}aggregate/combine_all_data.csv",index=False)


def upload_clean_data(stage):
    str_datetime = str(datetime.now().strftime("%Y-%m-%d"))
    file_location = f"/opt/airflow/data/transform/"
    files_name = file_in_path(file_location)
    bucket_name = "transfrom-us-east-1"
    object_location = f"{stage}/{str_datetime}/"
    
    #Loop upload all file in path
    for file in files_name:
        file_name = f"{file_location}{file}"
        object_name = f"{object_location}{file}"
        try:
            upload_file_s3(file_name,bucket_name,object_name)
        except Exception as e:
            print(e)

def upload_aggregate_data(stage):
    str_datetime = str(datetime.now().strftime("%Y-%m-%d"))
    file_location = f"/opt/airflow/data/aggregate/"
    files_name = file_in_path(file_location)
    bucket_name = "data-final-us-east-1"
    object_location = f"{stage}/{str_datetime}/"
    
    #Loop upload all file in path
    for file in files_name:
        file_name = f"{file_location}{file}"
        object_name = f"{object_location}{file}"
        try:
            upload_file_s3(file_name,bucket_name,object_name)
        except Exception as e:
            print(e)
    


