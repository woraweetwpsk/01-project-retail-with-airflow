from module.function import execute_mysql,connector
from module.create_database.create_simple_data import create_data_customers,create_data_products,create_data_sales

def create_table():
    #create table customers
    execute_mysql("CREATE TABLE customers (\
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
    
    #create table products
    execute_mysql("CREATE TABLE products(\
                        product_id INT PRIMARY KEY,\
                        product_name VARCHAR(100),\
                        category VARCHAR(50),\
                        price DECIMAL(10,2),\
                        stock_quantity INT,\
                        product_cost DECIMAL(10,2),\
                        product_profit DECIMAL(10,2))\
                        ")
    #create table sales
    execute_mysql("CREATE TABLE sales (\
                        sale_id INT PRIMARY KEY,\
                        customer_id INT,\
                        product_id INT,\
                        quantity INT,\
                        sale_date DATE,\
                        sale_time TIME,\
                        FOREIGN KEY (customer_id) REFERENCES customers(customer_id),\
                        FOREIGN KEY (product_id) REFERENCES products(product_id))\
                        ")
    
def create_customers_data():
    mydb = connector("mysql")
    mycursor = mydb.cursor()
    customers_data=create_data_customers()
    for row in customers_data:
        i = len(row)
        sql_c = f"INSERT INTO customers (customer_id,firstname,lastname,email,phone,address,province,country,zipcode) VALUES ({'%s, '* (i-1)}%s)"
        mycursor.execute(sql_c, tuple(row))
    mydb.commit()

def create_products_data():
    mydb = connector("mysql")
    mycursor = mydb.cursor()
    products_data=create_data_products()
    for row in products_data:
        i = len(row)
        sql = f"INSERT INTO products (product_id,product_name,category,price,stock_quantity,product_cost,product_profit) VALUES ({'%s, '* (i-1)}%s)"
        mycursor.execute(sql, tuple(row))
    mydb.commit()

def create_sales_data():
    mydb = connector("mysql")
    mycursor = mydb.cursor()
    sales_data = create_data_sales()
    for row in sales_data:
        i = len(row)
        sql = f"INSERT INTO sales (sale_id,customer_id,product_id,quantity,sale_date,sale_time) VALUES({'%s, '*(i-1)}%s)"
        mycursor.execute(sql, tuple(row))
    mydb.commit()

