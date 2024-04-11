from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.connection import s3_session, s3_client
from datetime import datetime, timedelta
from dotenv import load_dotenv
import botocore
import json
import os
import pytz
import requests
import urllib.parse

load_dotenv()

default_args = {
    "owner": "Jess",
    "retries": 5,
    "retry_delay": timedelta(minutes=5)
    }

def upload_to_S3(local_source_data_path, S3_file_path):
    with s3_session() as s3:
        result = s3.Bucket('for-side-project-demo').upload_file(local_source_data_path, S3_file_path)
        
def transform_datetime_format(**context):
    # get current time and ten minutes before current time
    time_zone = pytz.timezone('Asia/Taipei')

    current_time = datetime.now(time_zone)
    ten_mins_before_current_time = datetime.now() - timedelta(minutes=10)

    # transform datetime fomat to urllib.parse.quote format
    start_time = urllib.parse.quote(ten_mins_before_current_time.isoformat())
    end_time = urllib.parse.quote(current_time.isoformat())

    # push start_time and end_time to xcom
    context['ti'].xcom_push(key='start_time', value=start_time)
    context['ti'].xcom_push(key='end_time', value=end_time)

def write_source_json_data(local_source_data_path, json_object):
    with open(local_source_data_path, "w") as file:
        print("json_object", json_object)
        json.dump(json_object, file)
        
def file_exists_on_S3(key):
    with s3_client() as s3:
        try:
            s3.head_object(Bucket='for-side-project-demo', Key=key)
            print(f"Key: '{key}' found!")
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                print(f"Key: '{key}' does not exist!")
            else:
                print("Something else went wrong")
                raise


def task_get_earthquake_data(**context) -> dict:
    API_KEY = '***REMOVED***'

    start_time = context['ti'].xcom_pull(task_ids='transform_datetime_format', key='start_time')
    end_time = context['ti'].xcom_pull(task_ids='transform_datetime_format', key='end_time')

    
    url = f'https://opendata.cwa.gov.tw/api/v1/rest/datastore/E-A0015-001?Authorization={API_KEY}&timeFrom={start_time}&timeTo={end_time}'
    response = requests.get(url, headers = {"Accept":"application/json"})
    if response.status_code == 200:
        response_text = json.loads(response.text)
        earthquake_results = response_text['records']['Earthquake']
        for earthquake in earthquake_results:
            earthquake_number = earthquake['EarthquakeNo']
            local_source_data_path = f'/opt/airflow/data/earthquake_{earthquake_number}_source_data.json'

            S3_file_path = f"earthquake_source_data/earthquake_{earthquake_number}_source_data.json"
            write_source_json_data(local_source_data_path=local_source_data_path, json_object=earthquake)

            if os.path.isfile(local_source_data_path) and not file_exists_on_S3(key=S3_file_path):
                upload_to_S3(local_source_data_path, S3_file_path)



with DAG(
    dag_id='get_earthquake_raw_data',
    default_args=default_args,
    start_date=datetime(2024, 4, 7, 0),
    schedule_interval='@daily',
) as dag:
    
    task_transform_datetime_format = PythonOperator(
        task_id='task_transform_datetime_format',
        python_callable=transform_datetime_format
    )

    task_get_earthquake_data = PythonOperator(
        task_id='earthquake_data',
        python_callable=task_get_earthquake_data,
        provide_context=True
    )

    task_transform_datetime_format >> task_get_earthquake_data
