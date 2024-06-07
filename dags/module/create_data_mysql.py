import mysql.connector
import pandas as pd
import random 
import os
import configparser
from module.create_database.create_simple_data import create_data_customers,create_data_products,create_data_sales

def read_config():
    #Read config
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__),"config.ini"))
    db_host = config.get("db", "host")
    db_user = config.get("db", "user")
    db_password = config.get("db", "password")
    db_port = config.get("db","port")
    db_name = config.get("db","name")
    
    config_data = {"db_host" : db_host,
                   "db_user" : db_user,
                   "db_password" : db_password,
                   "db_port" : db_port,
                   "db_name" : db_name
                   }
    return config_data

def connection_sql():
    config_data = read_config()
    db_host = config_data["db_host"]
    db_user = config_data["db_user"]
    db_password = config_data["db_password"]
    db_port = config_data["db_port"]
    db_name = config_data["db_name"]
    #connection with mysql localhost
    mydb = mysql.connector.connect(
    host= db_host,
    user= db_user,
    password= db_password,
    port = db_port,
    database=db_name
    )
    
    return mydb

    # create table
def create_table():
    mydb = connection_sql()
    mycursor = mydb.cursor()
    mycursor.execute("CREATE TABLE customers (\
                        customer_id INT PRIMARY KEY,\
                        firstname VARCHAR(50),\
                        lastname VARCHAR(50),\
                        email VARCHAR(100),\
                        phone VARCHAR(20),\
                        address VARCHAR(100),\
                        province VARCHAR(50),\
                        country VARCHAR(50),\
                        zipcode VARCHAR(50))\
                        ")

    mycursor.execute("CREATE TABLE products(\
                        product_id INT PRIMARY KEY,\
                        product_name VARCHAR(100),\
                        category VARCHAR(50),\
                        price DECIMAL(10,2),\
                        stock_quantity INT,\
                        product_cost DECIMAL(10,2),\
                        product_profit DECIMAL(10,2))\
                        ")

    mycursor.execute("CREATE TABLE sales (\
                        sale_id INT PRIMARY KEY,\
                        customer_id INT,\
                        product_id INT,\
                        quantity INT,\
                        sale_date DATE,\
                        sale_time TIME,\
                        FOREIGN KEY (customer_id) REFERENCES customers(customer_id),\
                        FOREIGN KEY (product_id) REFERENCES products(product_id))\
                        ")
    
#Make Simple customers data
def create_customers_data():
    mydb = connection_sql()
    mycursor = mydb.cursor()
    customers_data=create_data_customers()
    for row_c in customers_data:
        i_c = len(row_c)
        sql_c = f"INSERT INTO customers (customer_id,firstname,lastname,email,phone,address,province,country,zipcode) VALUES ({'%s, '* (i_c-1)}%s)"
        mycursor.execute(sql_c, tuple(row_c))
    mydb.commit()

# Make Simple products data
def create_products_data():
    mydb = connection_sql()
    mycursor = mydb.cursor()
    products_data=create_data_products()
    for row_p in products_data:
        i_p = len(row_p)
        sql_p = f"INSERT INTO products (product_id,product_name,category,price,stock_quantity,product_cost,product_profit) VALUES ({'%s, '* (i_p-1)}%s)"
        mycursor.execute(sql_p, tuple(row_p))
    mydb.commit()

    # Make Simple Sales data
def create_sales_data():
    mydb = connection_sql()
    mycursor = mydb.cursor()
    sales_data = create_data_sales()
    for row_s in sales_data:
        i_s = len(row_s)
        sql_s = f"INSERT INTO sales (sale_id,customer_id,product_id,quantity,sale_date,sale_time) VALUES({'%s, '*(i_s-1)}%s)"
        mycursor.execute(sql_s, tuple(row_s))
    mydb.commit()

