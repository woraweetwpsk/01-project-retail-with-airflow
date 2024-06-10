import pandas as pd
import os
from io import StringIO
import configparser
import mysql.connector
import boto3


def get_config(x):
    #Read config
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__),"config.ini"))
    #Database
    db_host = config.get("db", "host")
    db_user = config.get("db", "user")
    db_password = config.get("db", "password")
    db_port = config.get("db","port")
    db_name = config.get("db","name")
    #AWS S3
    access_key_id = config.get("S3","access_key_id")
    secret_access_key = config.get("S3","secret_access_key")
    
    config_data = {"db_host" : db_host,
                   "db_user" : db_user,
                   "db_password" : db_password,
                   "db_port" : db_port,
                   "db_name" : db_name,
                   "access_key_id" : access_key_id,
                   "secret_access_key" : secret_access_key
                   }
    
    if x in config_data.keys():
        result = config_data[x]
    else:
        pass
        
    return result

def connector(server):
    if server == "mysql":
        db_host = get_config("db_host")
        db_user = get_config("db_user")
        db_password = get_config("db_password")
        db_port = get_config("db_port")
        db_name = get_config("db_name")
        #connection with mysql
        mydb = mysql.connector.connect(
        host= db_host,
        user= db_user,
        password= db_password,
        port = db_port,
        database=db_name
        )
        result = mydb
    
    elif server == "s3":
        access_key_id = get_config("access_key_id")
        secret_access_key = get_config("secret_access_key")
        session = boto3.Session(
        aws_access_key_id = access_key_id,
        aws_secret_access_key = secret_access_key
        )
        result = session
    else:
        pass
    return result

def execute_mysql(sql,data=None):
    mydb = connector("mysql")
    
    mycursor = mydb.cursor()
    try:
        if data == None:
            mycursor.execute(sql)
        else:
            mycursor.execute(sql, data)
        print("Execute in MySQL Database Complete")
    except Exception as e:
        print(e)

def upload_file_s3(file_name,bucket_name,object_name):
    s3_client = connector("s3").client("s3")
    try:
        s3_client.upload_file(file_name,bucket_name,object_name)
        print(f"File {file_name} was uploaded to {bucket_name}/{object_name}")
    except Exception as e :
        print(e)
    
# #Test upload file in S3
# file_name = os.path.abspath(os.path.join(os.path.dirname(__file__),"../../data/text.txt"))
# object_name = "text.txt"
# bucket_name = "project1forairflow"
# upload_file_s3(file_name,bucket_name,object_name)

def read_csv_s3(bucket_name,file_path):
    s3_client = connector("s3").client("s3")
    try:
        object_file = s3_client.get_object(Bucket=bucket_name, Key=file_path)['Body'].read().decode('utf-8')
        print(f"Get File {file_path} in {bucket_name} Complete")
    except Exception as e:
        print(e)
    dataframe = pd.read_csv(StringIO(object_file))
    return dataframe

# #Test read File
# bucket_name = "project1forairflow"
# file_path = "rawfile/full_load/2024-06-07_customers_rawdata.csv"
# x = read_csv_s3(bucket_name,file_path)
# print(x.head(5))

def file_in_path(path):
    files = []
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            files.append(filename)
    return files

def file_in_s3(bucket,path):
    s3_client = connector("s3").client("s3")
    paginator = s3_client.get_paginator('list_objects_v2')
    operation_parameters = {'Bucket': bucket, 'Prefix': path}
    
    file_list = []
    
    for page in paginator.paginate(**operation_parameters):
        if 'Contents' in page:
            for obj in page['Contents']:
                file_name = os.path.basename(obj['Key'])
                file_list.append(file_name)
    
    return file_list

# #Test List file in bucket
# bucket = "project1forairflow"
# path = "rawfile/full_load/2024-06-10"
# x = file_in_s3(bucket,path)
# print(x)

# bucket_name = "project1forairflow"
# object_name = "transform/full_load/2024-06-10/combine_all_data.csv"
# s3_client = connector("s3").client("s3")
# s3_client.download_file(bucket_name, object_name, "data/transform/test.csv")