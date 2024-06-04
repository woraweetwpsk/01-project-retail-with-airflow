import pandas as pd
import os
from datetime import datetime
from extract import read_config
from pyspark import SparkConf
from pyspark.sql import SparkSession


config_data = read_config()
access_key_id = config_data["access_key_id"]
secret_access_key = config_data["secret_access_key"]

conf = SparkConf()
conf.set("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.2")
conf.set("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
conf.set("spark.hadoop.fs.s3a.access.key", access_key_id)
conf.set("spark.hadoop.fs.s3a.secret.key", secret_access_key)

str_datetime = str(datetime.now().strftime("%Y-%m-%d"))

file_path = "s3://project1forairflow/rawfile/full_load/2024-06-04_customers_rawdata.csv"
spark = SparkSession.builder.config(conf=conf).getOrCreate()
df = spark.read.csv(file_path, header=True)
# customers_data = spark.read.csv("s3://project1forairflow/rawfile/full_load/2024-06-04_customers_rawdata.csv")
# products_data = spark.read.csv(f"s3://project1forairflow/rawfile/full_load/{str_datetime}_products_rawdata.csv")
# sales_data = spark.read.csv(f"s3://project1forairflow/rawfile/full_load/{str_datetime}_sales_rawdata.csv")

print(df.show(5))