import pandas as pd
import os
from datetime import datetime

from module.function import read_csv_s3,upload_file_s3,file_in_path,file_in_s3,connector

def import_file_s3(path,x):
    str_datetime = str(datetime.now().strftime("%Y-%m-%d"))
    bucket_name = "project1forairflow"
    # files_name = ["customers_rawdata.csv","products_rawdata.csv","sales_rawdata.csv"]
    file_path = f"{path}/full_load/{str_datetime}/"
    files_name = file_in_s3(bucket_name,file_path)
    
    if x in files_name:
        dataframe = read_csv_s3(bucket_name,f"{file_path}{x}")
    else:
        dataframe = None
        print(f"Cannot find the path {x} in Bucket {bucket_name}")
    
    return dataframe
    
def cleansing_data():
    customers_df = import_file_s3("rawfile","customers_rawdata.csv")
    products_df = import_file_s3("rawfile","products_rawdata.csv")
    sales_df = import_file_s3("rawfile","sales_rawdata.csv")
    
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
    customers_df.to_csv(f"{path}customers_transform_data.csv",index=False)
    products_df.to_csv(f"{path}products_transform_data.csv",index=False)
    sales_df.to_csv(f"{path}sales_transform_data.csv",index=False)
    combine_all_df.to_csv(f"{path}combine_all_data.csv",index=False)

def upload_clean_data():
    str_datetime = str(datetime.now().strftime("%Y-%m-%d"))
    file_location = f"/opt/airflow/data/transform/"
    # files_name = [i for x in os.walk(file_location) for i in x[-1]]
    files_name = file_in_path(file_location)
    bucket_name = "project1forairflow"
    object_location = f"transform/full_load/{str_datetime}/"
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
                

def aggregate_data():
    all_data = import_file_s3("transform","combine_all_data.csv")
    data_income = all_data[["sale_id","sale_date","price","quantity","product_profit"]]
    data_income["sale_date"] = pd.to_datetime(data_income["sale_date"])
    
    data_income["total_income"] = data_income["price"] * data_income["quantity"]
    data_income["total_profit"] = data_income["product_profit"] * data_income["quantity"]

    data_income = data_income[["sale_date","sale_id","total_income","total_profit"]]
    final_income = data_income.groupby(by="sale_date").agg({"sale_id":"count","total_income":"sum","total_profit":"sum"})

    final_income.to_csv(f"/opt/airflow/data/aggregate/data_income.csv",index=False)
    # return final_income

def upload_aggregate_data():
    str_datetime = str(datetime.now().strftime("%Y-%m-%d"))
    file_location = f"/opt/airflow/data/aggregate/"
    # files_name = [i for x in os.walk(file_location) for i in x[-1]]
    files_name = file_in_path(file_location)
    bucket_name = "project1forairflow"
    object_location = f"aggregate/full_load/{str_datetime}/"
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

# x = aggregate_data()
# print(x)


