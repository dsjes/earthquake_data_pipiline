from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.connection import s3_session, s3_client, postgres_conn
from datetime import datetime, timedelta
import pytz
import re

default_args = {"owner": "Jess", "retries": 5, "retries_interval": timedelta(minutes=5)}


def get_all_s3_objects():
    with s3_session() as s3:
        s3_bucket = s3.Bucket("for-side-project-demo")
        bucket_object_list = [
            bucket_object.key.replace("earthquake_source_data/", "")
            for bucket_object in s3_bucket.objects.all()
        ]
        return bucket_object_list


def insert_earthquake_report_status():
    bucket_object_list = get_all_s3_objects()
    with postgres_conn() as cursor:
        for bucket_object in bucket_object_list:
            match = re.search(r"\d+", bucket_object)
            time_zone = pytz.timezone("Asia/Taipei")
            current_time = datetime.now(time_zone)
            if bucket_object != "" and match:
                cursor.execute(
                    "INSERT INTO earthquake_report_status (earthquake_report_id, status, insert_time) VALUES (%s, %s, %s)",
                    (match.group(), 0, current_time),
                )
            else:
                raise ValueError


with DAG(
    dag_id="transformed_earthquake_raw_data",
    default_args=default_args,
    start_date=datetime(2024, 4, 5, 0),
    schedule_interval="@daily",
) as dag:
    task_get_all_s3_objects = PythonOperator(
        task_id="task_get_all_s3_objects",
        python_callable=get_all_s3_objects,
    )

    task_insert_earthquake_report_status = PythonOperator(
        task_id="task_insert_earthquake_report_status",
        python_callable=insert_earthquake_report_status,
    )
