import os
import csv

from module.function import connector,search_new_path,file_in_s3,execute_mysql


def load_aggregate(folder):
    s3 = connector("s3").client("s3")
    bucket_name = "data-final-us-east-1"
    file_path = search_new_path(bucket_name,f"{folder}/")
    file_name = file_in_s3(bucket_name,file_path)
    download_path = "/opt/airflow/data/aggregate/"
    #Loop Download File in Path
    for name in file_name:
        try:
            s3.download_file(bucket_name, f"{file_path}/{name}", f"{download_path}/{name}")
            print(f"Download file {name} in {file_path} complete")
        except Exception as e :
            print(e)
            
def create_table_data():
    execute_mysql("CREATE TABLE alldata(sale_id INT PRIMARY KEY,\
                                    customer_id INT,\
                                    product_id INT,\
                                    quantity INT,\
                                    sale_date DATE,\
                                    sale_time VARCHAR(50),\
                                    product_name VARCHAR(100),\
                                    category VARCHAR(100),\
                                    price DECIMAL(10,2),\
                                    stock_quantity INT,\
                                    product_cost DECIMAL(10,2),\
                                    product_profit DECIMAL(10,2),\
                                    firstname VARCHAR(100),\
                                    lastname VARCHAR(100),\
                                    email VARCHAR(100),\
                                    phone VARCHAR(50),\
                                    address VARCHAR(10),\
                                    province VARCHAR(50),\
                                    country VARCHAR(50),\
                                    zipcode VARCHAR(10))\
                        ")

def upload_to_mysql():
    download_path = "/opt/airflow/data/aggregate/"
    name = "combine_all_data.csv"
    conn = connector("mysql")
    cursor = conn.cursor()
    
    with open(f"{download_path}/{name}", newline='') as csvfile:
        csvreader = csv.reader(csvfile)
        header = next(csvreader)  #Read Header

        insert_query = "INSERT INTO alldata ({}) VALUES ({})".format(
        ','.join(header),
        ','.join(['%s'] * len(header))
        )

        #Loop query Data in mysql
        for row in csvreader:
            cursor.execute(insert_query, row)

    conn.commit()
    cursor.close()
    conn.close()
    
    #Delete Path in Server
    if os.path.exists(f"{download_path}/{name}"):
        os.remove(f"{download_path}/{name}")
        print(f"the file {name} has been delete.")
    else:
        print(f"the file {name} does not exist.")
        
