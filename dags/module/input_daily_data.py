import pandas as pd

from module.function import execute_mysql,connector
from module.create_database.create_simple_data import input_daily_data


def _input_daily_data():
    mydb = connector("mysql")
    mycursor = mydb.cursor()
    
    #ดึงลำดับ sale_id ล่าสุด
    sales = pd.read_sql("SELECT max(sale_id) as max_sale_id FROM sales",mydb)
    max_sale_id = int(sales["max_sale_id"].iloc[0])+1
    sales_data = input_daily_data(max_sale_id)
    
    for row in sales_data:
        i = len(row)
        sql = f"INSERT INTO sales (sale_id,customer_id,product_id,quantity,sale_date,sale_time) VALUES({'%s, '*(i-1)}%s)"
        mycursor.execute(sql, tuple(row))
    mydb.commit()
