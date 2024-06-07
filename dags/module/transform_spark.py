import pandas as pd
import configparser
import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from datetime import datetime

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

def transform_data():
    config_data = read_config()
    access_key_id = config_data["access_key_id"]
    secret_access_key = config_data["secret_access_key"]
    
    #Set Connection and Spark
    conf = (
    SparkConf()
    .setAppName("Transform_data") 
    .set("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.2")
    .set("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.hadoop.fs.s3a.access.key", access_key_id)
    .set("spark.hadoop.fs.s3a.secret.key", secret_access_key)
    .set("spark.sql.shuffle.partitions", "200") 
    .setMaster("local[*]")
    )
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    
    #read file in S3
    str_datetime = str(datetime.now().strftime("%Y-%m-%d"))
    customers_dt = spark.read.format('csv').load(f's3a://project1forairflow/rawfile/full_load/{str_datetime}_customers_rawdata.csv',header=True)
    products_dt = spark.read.format('csv').load(f's3a://project1forairflow/rawfile/full_load/{str_datetime}_products_rawdata.csv',header=True)
    sales_dt = spark.read.format('csv').load(f's3a://project1forairflow/rawfile/full_load/{str_datetime}_sales_rawdata.csv',header=True)
    
    #Change Schema
    customers_dt = customers_dt.withColumns({"customer_id" :f.col("customer_id").cast("int")})
    products_dt = products_dt.withColumns({"product_id" :f.col("product_id").cast("int"),
                                           "price" :f.col("price").cast("double"),
                                           "stock_quantity" :f.col("stock_quantity").cast("int"),
                                           "product_cost" :f.col("product_cost").cast("double"),
                                           "product_profit" :f.col("product_profit").cast("double"),
                                           })
    sales_dt = sales_dt.withColumns({"sale_id" :f.col("sale_id").cast("int"),
                                     "customer_id" :f.col("customer_id").cast("int"),
                                     "product_id" :f.col("product_id").cast("int"),
                                     "quantity" :f.col("quantity").cast("int"),
                                     "sale_date" :f.col("sale_date").cast("date"),
                                     })
    
    #Substring ("0 days")
    sales_dt_clean = sales_dt.withColumn("sale_time", f.substring("sale_time",7,15))
    
    #Join all data
    alldata = sales_dt_clean.join(products_dt, "product_id").join(customers_dt, "customer_id")
    
    #Data Aggregation
    alldata.createOrReplaceTempView("alldata")
    
    #income per day
    income_data_per_day = spark.sql("SELECT sale_date,\
                                      count(sale_id) as bill_per_day,\
                                      sum(quantity*price) as income_per_day,\
                                      round(sum(quantity*product_profit),2) as profit_per_day\
                                      FROM alldata\
                                      GROUP BY sale_date\
                                      ORDER BY sale_date\
                                ")
    
    #Customer Spend Data
    customers_data_spend = spark.sql("SELECT customer_id,\
                                      sum(customer_id) as total_order,\
                                      sum(quantity*price) as total_spend\
                                      FROM alldata\
                                      GROUP BY customer_id\
                                      ORDER BY customer_id\
                                    ")
    
    income_data_per_day.show(10)
    
transform_data()
    
    #Upload in S3 path
    # s3_path = f"s3a://project1forairflow/transition/{str_datetime}/"
    # alldata.write.mode("overwrite").format("csv").option("header",True).save(f"{s3_path}alldata")
    # customers_data_spend.write.mode("overwrite").format("csv").option("header",True).save(f"{s3_path}customer_spend")
    # income_data_per_day.write.mode("overwrite").format("csv").option("header",True).save(f"{s3_path}income_per_day")
