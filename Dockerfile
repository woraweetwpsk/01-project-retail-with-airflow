FROM apache/airflow:2.9.1
RUN pip install pyspark

COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install -r /opt/airflow/requirements.txt

WORKDIR /opt/airflow/


