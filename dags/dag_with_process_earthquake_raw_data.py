from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.connection import s3_session, s3_client
from datetime import datetime, timedelta

default_args = {
    "owner":"Jess",
    "retries":5,
    "retries_interval":timedelta(minutes=5)
}

def insert_earthquake_report_data():
    pass
        
def load_area_data():
    with s3_session() as s3:
        pass
        
with DAG(
    dad_id="collect_area_data",
    default_args=default_args,
    start_date=datetime(2024,4,5,0),
    schedule_interval='@daily'
) as dag:
    pass