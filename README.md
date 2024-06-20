## **01 - Project Retail**

เป็น Project ในการสร้างฐานข้อมูลแบบ random ขึ้นมาแล้วนำมาทำโปรเจคเกี่ยวกับร้านขายของทั่วไป

โดยแบ่งเป็นการทำ pipeline แบบ full load คือนำเข้าทั้งหมด และ update ข้อมูลเป็น daily

แล้วจึงนำไปทำ Cleansing และ Transform ข้อมูลก่อนที่จะนำไปทำ Dashboard

โดยจะวิเคราะห์เป็น รายรับต่อวัน, กำไรต่อวัน, จำนวนสินค้าที่ขายได้โดยแบ่งตามประเภท และยอดรวมการใช้เงินของลูกค้า

---

### Architecture

![A](https://github.com/woraweetwpsk/01-project-retail-with-airflow/blob/main/images/a.png?raw=true)

---

### Tool ที่ใช้ใน Project

- Database, Data Warehouse = MySQL Server
- Data Lake = AWS S3
- Data organization = Apache Airflow
- Cleansing Data = Pandas
- Dashboard = Amazon Quicksight

---

### ER diagram

![ER_diagram](https://github.com/woraweetwpsk/01-project-retail-with-airflow/assets/167957460/6e2505ca-7f32-4e69-9e7f-031cd6b5e7b0)

---

### DAG
- **Full load**
  - สร้างฐานข้อมูลลงใน Database
  - ดึงข้อมูลมาเก็บใน Bucket : raw-file
  - Cleansing และ Transform ข้อมูล แล้วเก็บใน Bucket : transfom , final-data
  - นำข้อมูลที่ Transform แล้ว upload ไปยัง mysql (Data Warehouse)
- **Daily**
  - สร้างข้อมูลยอดขายรายวันแล้วเพิ่มไปใน Database
  - ดึงข้อมูลในวันปัจจุบันมาเก็บใน Bucket : raw-file
  - Cleansing และ Transform ข้อมูล แล้วเก็บใน Bucket : transfom , final-data
  - นำข้อมูลที่ Transform แล้ว updata ข้อมูลไปยัง mysql (Data Warehouse) โดยอัพเดทเฉพาะวันปัจจุบัน
---

### Dashboard

![Dashboard](https://github.com/woraweetwpsk/01-project-retail-with-airflow/blob/main/images/dashboard.png?raw=true)

---
