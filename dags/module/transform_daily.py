import pandas as pd
import os
from datetime import datetime

from module.function import read_csv_s3,file_in_s3,file_in_path,upload_file_s3

def import_file_s3(stage,file_name):
    str_datetime = str(datetime.now().strftime("%Y-%m-%d"))
    
    bucket_name = "project1forairflow"
    
    file_path_daily = f"rawfile/daily/{str_datetime}/"
    daily_name = "daily_sale.csv"

    full_load_path = "transform/full_load/2024-06-10/"
    full_load_name = file_in_s3(bucket_name,full_load_path)
    if stage == "cleansing":
        if file_name in full_load_name:
            dataframe = read_csv_s3(bucket_name,f"{full_load_path}{file_name}")
        elif file_name == daily_name:
            dataframe = read_csv_s3(bucket_name,f"{file_path_daily}{file_name}")
        else:
            print(f"Unable to find file : {file_name}")
            
    elif stage == "agg":
        file_path = f"transform/daily/{str_datetime}/"
        file_name = file_in_s3(bucket_name,file_in_path)
    
    else:
        print(f"Unable to find stage : {stage}")
        
    return dataframe

def cleansing_daily_data():
    sales_daily = import_file_s3("cleansing","daily_sale.csv")
    
    sales_daily["sale_date"] = pd.to_datetime(sales_daily["sale_date"])
    sales_daily["sale_time"] = sales_daily["sale_time"].str.slice(start = 7)
    
    customers_transform = import_file_s3("cleansing","customers_transform_data.csv")
    products_transform = import_file_s3("cleansing","products_transform_data.csv")

    combine_all_df = pd.merge(pd.merge(sales_daily,products_transform,how="left",on="product_id"),customers_transform,how="left",on="customer_id")
    
    path = "/opt/airflow/data/transform/"
    combine_all_df.to_csv(f"{path}combine_all_data.csv",index=False)
    sales_daily.to_csv(f"{path}transform_daily_sale.csv",index=False)
    
def upload_clean_daily():
    str_datetime = str(datetime.now().strftime("%Y-%m-%d"))
    file_location = f"/opt/airflow/data/transform/"
    # files_name = [i for x in os.walk(file_location) for i in x[-1]]
    files_name = file_in_path(file_location)
    bucket_name = "project1forairflow"
    object_location = f"transform/daily/{str_datetime}/"
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

def aggregate_daily():
    all_data = import_file_s3("agg","combine_all_data.csv")
    data_income = all_data[["sale_id","sale_date","price","quantity","product_profit"]]
    data_income["sale_date"] = pd.to_datetime(data_income["sale_date"])
    
    data_income["total_income"] = data_income["price"] * data_income["quantity"]
    data_income["total_profit"] = data_income["product_profit"] * data_income["quantity"]

    data_income = data_income[["sale_date","sale_id","total_income","total_profit"]]
    final_income = data_income.groupby(by="sale_date").agg({"sale_id":"count","total_income":"sum","total_profit":"sum"})

    final_income.to_csv(f"/opt/airflow/data/aggregate/daily_data_income.csv",index=False)
    # return final_income

def upload_aggregate_daily():
    str_datetime = str(datetime.now().strftime("%Y-%m-%d"))
    file_location = f"/opt/airflow/data/aggregate/"
    # files_name = [i for x in os.walk(file_location) for i in x[-1]]
    files_name = file_in_path(file_location)
    bucket_name = "project1forairflow"
    object_location = f"aggregate/daily/{str_datetime}/"
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

# read_csv_s3(bucket_name,file_path)