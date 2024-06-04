import pandas as pd
import random
import datetime

#for random data customers
firstnames = ['Achara', 'Apinya', 'Arthit', 'Boonma', 'Chaiya', 'Dara', 'Kanya', 'Kla', 'Mali', 'Niran', 'Phu', 'Pranee', 'Somsak', 'Udom', 'Wipa', 'Somchai', 'Siri', 'Sukanya', 'Suthida', 'Warin', 'Ying', 'Nattapong', 'Pattara', 'Thanya', 'Thongchai']
lastnames = ['Aroon', 'Bunma', 'Chatri', 'Kong', 'Laor', 'Mang', 'Nim', 'Preecha', 'Saithong', 'Thongchai', 'Virote', 'Watanabe', 'Boonyarit', 'Chankham', 'Hiran', 'Kiat', 'Lert', 'Maneerut', 'Rattanapong', 'Somboon', 'Worachot', 'Yotsawat']
provinces = ['Bangkok', 'Chiang Mai', 'Phuket', 'Khon Kaen', 'Udon Thani', 'Chonburi', 'Nakhon Ratchasima', 'Surat Thani', 'Pattani', 'Krabi', 'Ayutthaya', 'Lampang', 'Ubon Ratchathani', 'Rayong', 'Nakhon Si Thammarat']
zipcodes = [str(random.randint(10000, 99999)) for _ in range(100)]

def create_data_customers():
    data = []
    for i in range (1, 101):
        firstname = random.choice(firstnames)
        lastname = random.choice(lastnames)
        email = f"{firstname.lower()}.{lastname.lower()}@example.com"
        phone = f"0{random.randint(800000000, 999999999)}"
        address = f"{random.randint(1,999)}/{random.randint(1,999)}"
        province = random.choice(provinces)
        country = "Thailand"
        zipcode = random.choice(zipcodes)
        
        data.append([i, firstname, lastname, email, phone, address, province, country, zipcode])
        
    return data

#for data products
raw_data_products= {'Beverages': {'Coca Cola', 'Pepsi', 'Sprite', 'Fanta', 'Red Bull', 'Lipton Iced Tea', 'Green Tea', 'Mineral Water', 'Orange Juice', 'Apple Juice', 'Energy Drink', 'Coffee', 'Green Tea Latte', 'Milk', 'Soy Milk', 'Chocolate Milk', 'Sports Drink', 'Coconut Water', 'Lemonade', 'Smoothie'},
'Snacks': {'Lays Chips', 'Pringles', 'Doritos', 'Cheetos', 'Oreo Cookies', 'KitKat', 'Snickers', 'Mars', 'Twix', 'M&M s', 'Skittles', 'Gummy Bears', 'Pretzels', 'Popcorn', 'Rice Crackers', 'Nuts', 'Trail Mix', 'Granola Bars', 'Biscuits', 'Cheese Puffs'},
'Personal Care': {'Shampoo', 'Conditioner', 'Body Wash', 'Soap', 'Toothpaste', 'Toothbrush', 'Deodorant', 'Shaving Cream', 'Razor Blades', 'Face Wash', 'Moisturizer', 'Lip Balm', 'Hand Sanitizer', 'Cotton Swabs', 'Sunscreen', 'Perfume', 'Hair Gel', 'Hair Spray', 'Lotion', 'Nail Polish'},
'Household': {'Detergent', 'Fabric Softener', 'Dish Soap', 'Sponges', 'Paper Towels', 'Toilet Paper', 'Trash Bags', 'Aluminum Foil', 'Cling Wrap', 'Cleaning Spray', 'Bleach', 'Air Freshener', 'Batteries', 'Light Bulbs', 'Matches', 'Candles', 'Mop', 'Broom', 'Dustpan', 'Laundry Basket'},
'Food': {'Instant Noodles', 'Rice', 'Pasta', 'Bread', 'Butter', 'Cheese', 'Eggs', 'Milk', 'Yogurt', 'Cereal', 'Oatmeal', 'Peanut Butter', 'Jam', 'Honey', 'Frozen Pizza', 'Frozen Vegetables', 'Fresh Vegetables', 'Fresh Fruits', 'Canned Beans', 'Canned Tuna'}}

def create_data_products():
    count=1
    data=[]
    for key,vals in raw_data_products.items():
        for val in vals:
            product_id = count
            product_name = key
            category = val
            price = float(random.randrange(20, 80,5))
            stock_quantity = random.randrange(500,1000,50)
            random_percent = round(random.uniform(1.05,1.10),2)
            product_cost = round(price/random_percent,2)
            product_profit = round(price-product_cost,2)
            data.append([product_id,product_name,category,price,stock_quantity,product_cost,product_profit])
            count+=1
    return data
#Create Sale date

def create_data_sales():
    sales_data = []
    sale_id = 1
    #run date start to (today - 1 )
    start_date = datetime.date(2024,1,1)
    end_date = datetime.date.today() - datetime.timedelta(days = 1)
    dif_day = (end_date-start_date).days
    for i in range(dif_day):
        sale_date = (start_date + datetime.timedelta(days = i)).strftime("%Y-%m-%d")
        #random range customers per day
        random_number_of_customers = random.randrange(20,100)
        
        for i in range(random_number_of_customers):
            customer_id = random.randrange(1,100)
            product_id = random.randrange(1,100)
            quantity = random.randrange(1,5)
            #randomtime
            random_time = f"{random.randrange(8,17)}:{random.randrange(0,59)}:{random.randrange(0,59)}"
            
            #เพิ่มข้อมูลลงใน list => sales_datas
            #โดยยังไม่ได้เรียงวันและเวลา และ sale id = 1 ทั้งหมด
            data_day=[sale_id,customer_id,product_id,quantity,sale_date,random_time]
            sales_data.append(data_day)

    #เรียงวันและเวลาโดยใช้ pandas Dataframe
    cols = ['sale_id','customer_id','product_id','quantity','sale_date','sale_time']
    df = pd.DataFrame(sales_data,columns=cols)
    #.set_index(["sale_id"])
    df.sort_values(["sale_date","sale_time"])
    
    #convert Dataframe to List
    sales_data=df.values.tolist()
    
    #จำนวน sale id
    number_sale=len(sales_data)
    #Run เลข sale_id ใหม่ให้ตามลำดับ
    while sale_id<=number_sale:
        for i in sales_data:
            i[0]=sale_id
            sale_id+=1
    
    return sales_data
