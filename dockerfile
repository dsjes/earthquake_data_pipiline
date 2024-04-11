FROM apache/airflow:2.8.4
COPY . /opt/airflow
WORKDIR /opt/airflow
RUN pip install -r requirements.txt