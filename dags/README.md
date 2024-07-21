### รายละเอียดไฟล์ต่างๆใน Folder

1. module / : เป็น folder ที่เก็บ Script function python ที่ไว้ใช้กับไฟล์ dag
   - create_database/create_simple_data.py : เป็นไฟล์สำหรับการสุ่มข้อมูลทำ Mockup data
   - create_data.py : เป็นไฟล์เก็บ function สำหรับการสร้างข้อมูลบน Database ย้อนหลัง
   - input_daily_data.py : เป็นไฟล์เก็บ function สำหรับการสร้างข้อมูลบน Database เฉพาะวันปัจจุบัน
   - function.py : เป็นไฟล์เก็บ function ต่างๆที่ใช้บ่อย จึงเขียนแยกออกมาเป็นไฟล์นึง
   - extract.py : เป็นไฟล์เก็บ function สำหรับดึงข้อมูลจาก database และ อัพโหลดไปยัง S3
   - transform.py : เป็นไฟล์เก็บ function สำหรับ transform ข้อมูลจาก S3
   - load.py : เป็นไฟล์เก็บ function สำหรับนำข้อมูลจาก S3 ที่ Transform แล้ว upload ไปยัง database
2. dag_create_daily_data.py : เป็นไฟล์ DAG ในการสร้างข้อมูลอัพเดทรายวัน
3. dag_create_data.py : เป็นไฟล์ DAG ในการสร้างข้อมูล Mock up ย้อนหลัง
4. dag_daily_pipeline.py : เป็นไฟล์ DAG ของ pipeline ที่ทำงานกับข้อมูลที่อัพเดทรายวัน
5. dag_pipeline.py : เป็นไฟล์ DAG ของ pipeline ที่ทำงานกับข้อมูลทั้งหมดที่นำเข้าแบบ full load
