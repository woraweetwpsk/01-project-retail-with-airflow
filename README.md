# 01 - Project Retail
### Tool ที่ใช้ใน Project
- Database = MySQL Server
- Data Lake = AWS S3
- Data Pipeline and Monitor = Apache Airflow
- Cleansing Data = Pandas
---
### ข้อมูลที่นำมาใช้
โปรเจคนี้เป็นการสร้างข้อมูลแบบสุ่มขึ้นมาเพื่อใช้ในการสร้าง Data Pipeline.
โดยจะมีแบ่งสุ่มเป็น 3 รายการ คือ
- รายการ Customer สุ่มฐานลูกค้าจำนวน 100 คน
- รายการ Product สุ่มรายการสินค้าจำนวน 100 ชิ้น
- รายการ Sale สุ่มรายการขายในแต่ละวัน
---
### โดยจะมีการแบ่ง เป็น 2 ช่วง
### ช่วงที่ 1 ETL Pipeline ข้อมูลทั้งหมด
1. สร้างฐานข้อมูล customer ,product และ sale โดยเริ่มตั้งแต่วันที่ 2024-01-01 จนถึงก่อนวันปัจจุบัน เป็นฐานข้อมูลตั้งต้น
2. นำเข้าข้อมูลลงใน Database
3. นำข้อมูลจาก Database ไปสู่ Data Lake นำเข้าใน Zone : Raw File
4. นำข้อมูลจาก Zone : Raw File มา Cleansing ข้อมูล และ Upload ไปยัง Zone : Transform
5. นำข้อมูลไป Aggregate โดยมีข้อมูลทั้งหมดที่นำมา join กัน และข้อมูลสรุปรายรับต่อวัน, กำไรต่อวัน
### ช่วงที่ 2 ETL Pipeline ข้อมูลใหม่รายวัน
1. นำเข้าข้อมูลการขายต่อวันจาก Database ไปยัง Data Lake ใน Zone : Raw File
2. Cleansing ข้อมูลแล้ว Upload ไปยัง Zone :Tramsform
3. นำข้อมูลไป Aggregate เพื่ออัพเดทรายวัน