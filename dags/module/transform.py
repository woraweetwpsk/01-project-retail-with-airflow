import pandas as pd
import os
from datetime import datetime

from module.function import read_csv_s3,upload_file_s3,file_in_path,file_in_s3,search_new_path

    
def cleansing_data():
    bucket_name = "project1forairflow"
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
    
    #Make Join all Data 
    combine_all_df = pd.merge(pd.merge(sales_df,products_df,how="left",on="product_id"),customers_df,how="left",on="customer_id")
    
    #Save file in path
    path = "/opt/airflow/data/transform/"
    # path = "data/transform/"
    customers_df.to_csv(f"{path}customers_transform_data.csv",index=False)
    products_df.to_csv(f"{path}products_transform_data.csv",index=False)
    sales_df.to_csv(f"{path}sales_transform_data.csv",index=False)
    combine_all_df.to_csv(f"{path}combine_all_data.csv",index=False)


def cleansing_daily_data():
    str_datetime = str(datetime.now().strftime("%Y-%m-%d"))
    bucket_name = "project1forairflow"
    lasted_path_full_load = search_new_path(bucket_name,"transform/full_load/")
    path_daily = f"rawfile/daily/{str_datetime}/"
    
    sales_daily = read_csv_s3(bucket_name,f"{path_daily}daily_sale.csv")
    customers_transform = read_csv_s3(bucket_name,f"{lasted_path_full_load}/customers_transform_data.csv")
    products_transform = read_csv_s3(bucket_name,f"{lasted_path_full_load}/products_transform_data.csv")
    
    combine_all_df = pd.merge(pd.merge(sales_daily,products_transform,how="left",on="product_id"),customers_transform,how="left",on="customer_id")
    
    path = "/opt/airflow/data/transform/"
    combine_all_df.to_csv(f"{path}combine_all_data.csv",index=False)
    sales_daily.to_csv(f"{path}transform_daily_sale.csv",index=False)


def upload_clean_data(stage):
    str_datetime = str(datetime.now().strftime("%Y-%m-%d"))
    file_location = f"/opt/airflow/data/transform/"
    files_name = file_in_path(file_location)
    bucket_name = "project1forairflow"
    object_location = f"transform/{stage}/{str_datetime}/"
    for file in files_name:
        file_name = f"{file_location}{file}"
        object_name = f"{object_location}{file}"
        try:
            upload_file_s3(file_name,bucket_name,object_name)
        except Exception as e:
            print(e)
                

def func_agg_data(stage):
    bucket_name = "project1forairflow"
    str_datetime = str(datetime.now().strftime("%Y-%m-%d"))
    all_data = read_csv_s3(bucket_name,f"transform/{stage}/{str_datetime}/combine_all_data.csv")
    data_income = all_data[["sale_id","sale_date","price","quantity","product_profit"]]
    data_income["sale_date"] = pd.to_datetime(data_income["sale_date"])
    
    data_income["total_income"] = data_income["price"] * data_income["quantity"]
    data_income["total_profit"] = data_income["product_profit"] * data_income["quantity"]

    data_income = data_income[["sale_date","sale_id","total_income","total_profit"]]
    final_income = data_income.groupby(by="sale_date").agg({"sale_id":"count","total_income":"sum","total_profit":"sum"})

    final_income.to_csv(f"/opt/airflow/data/aggregate/data_income.csv",index=False)


def func_upload_agg(stage):
    str_datetime = str(datetime.now().strftime("%Y-%m-%d"))
    file_location = f"/opt/airflow/data/aggregate/"
    # files_name = [i for x in os.walk(file_location) for i in x[-1]]
    files_name = file_in_path(file_location)
    bucket_name = "project1forairflow"
    object_location = f"aggregate/{stage}/{str_datetime}/"
    for file in files_name:
        file_name = f"{file_location}{file}"
        object_name = f"{object_location}{file}"
        try:
            upload_file_s3(file_name,bucket_name,object_name)
        except Exception as e:
            print(e)
    


