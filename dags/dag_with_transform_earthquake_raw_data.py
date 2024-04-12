from airflow import DAG
from airflow.operators.python import PythonOperator
from config.connection import s3_session, s3_client, postgres_conn
from datetime import datetime, timedelta
import pytz
import re
import json

default_args = {"owner": "Jess", "retries": 5, "retries_interval": timedelta(minutes=5)}


def get_all_s3_objects():
    # get all object key from s3
    with s3_session() as s3:
        s3_bucket = s3.Bucket("for-side-project-demo")
        bucket_object_list = [
            bucket_object.key.replace("earthquake_source_data/", "")
            for bucket_object in s3_bucket.objects.all()
            if ".json" in bucket_object.key
        ]
        return bucket_object_list


def fetch_data_from_s3(key):
    # get earthquake source data from s3
    with s3_client() as s3:
        obj = s3.get_object(Bucket="for-side-project-demo", Key=key)
        data = obj["Body"].read()
        result = json.loads(data.decode("utf-8"))
        print(f"result:{result}")
        return result


def collect_earthquake_report_row_data(data, current_time):
    # collect earthquake_report row data from fetch_data_from_s3 output
    earthquake_number = data["EarthquakeNo"]
    report_type = data["ReportType"]
    report_color = data["ReportColor"]
    report_content = data["ReportContent"]
    report_image_uri = data["ReportImageURI"]
    report_remark = data["ReportRemark"]
    web_uri = data["Web"]
    shakemap_image_uri = data["ShakemapImageURI"]
    insert_time = current_time
    return (
        earthquake_number,
        report_type,
        report_color,
        report_content,
        report_image_uri,
        report_remark,
        web_uri,
        shakemap_image_uri,
        insert_time,
    )


def collect_earthquake_info(data, current_time):
    # collect earthquake_info row data from fetch_data_from_s3 output
    earthquake_number = data["EarthquakeNo"]
    origin_time = data["EarthquakeInfo"]["OriginTime"]
    source = data["EarthquakeInfo"]["Source"]
    focal_depth = data["EarthquakeInfo"]["FocalDepth"]
    location = data["EarthquakeInfo"]["Epicenter"]["Location"]
    epicenter_latitude = data["EarthquakeInfo"]["Epicenter"]["EpicenterLatitude"]
    epicenter_longtitude = data["EarthquakeInfo"]["Epicenter"]["EpicenterLongitude"]
    magnitude_type = data["EarthquakeInfo"]["EarthquakeMagnitude"]["MagnitudeType"]
    magnitude_value = data["EarthquakeInfo"]["EarthquakeMagnitude"]["MagnitudeValue"]
    insert_time = current_time
    return (
        earthquake_number,
        origin_time,
        source,
        focal_depth,
        location,
        epicenter_latitude,
        epicenter_longtitude,
        magnitude_type,
        magnitude_value,
        insert_time,
    )


def collect_earthquake_report_status(bucket_object):
    match = re.search(r"\d+", bucket_object)
    time_zone = pytz.timezone("Asia/Taipei")
    current_time = datetime.now(time_zone)
    if match:
        return (match.group(), "0", current_time)


def collect_all_tables_data(bucket_object):
    data = fetch_data_from_s3(key=f"earthquake_source_data/{bucket_object}")
    time_zone = pytz.timezone("Asia/Taipei")
    current_time = datetime.now(time_zone)
    earthquake_report_row_data = collect_earthquake_report_row_data(data, current_time)
    earthquake_info_row_data = collect_earthquake_info(data, current_time)
    earthquake_report_status_row_data = collect_earthquake_report_status(bucket_object)
    return (
        earthquake_report_row_data,
        earthquake_info_row_data,
        earthquake_report_status_row_data,
    )


def get_all_columns(table_name):
    with postgres_conn() as (connection, cursor):
        sql_query = """
            SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = %s
            """
        cursor.execute(sql_query, (table_name,))
        columns_list = [row[0] for row in cursor.fetchall()][1:]
        print(columns_list)
        return columns_list


def insert_table_query(table_name):
    columns_list = get_all_columns(table_name)
    columns_string = ", ".join(columns_list)
    placeholders = ", ".join(["%s"] * (len(columns_list)))
    sql_query = f"""INSERT INTO {table_name} ({columns_string}) 
    VALUES ({placeholders})
    ON CONFLICT DO NOTHING"""
    return sql_query


def insert_new_row_data():
    with postgres_conn() as (connection, cursor):
        bucket_object_list = get_all_s3_objects()
        for bucket_object in bucket_object_list:
            (
                earthquake_report_row_data,
                earthquake_info_row_data,
                earthquake_report_status_row_data,
            ) = collect_all_tables_data(bucket_object)
            table_name_row_data_dict = {
                "earthquake_report": earthquake_report_row_data,
                "earthquake_info": earthquake_info_row_data,
                "earthquake_report_status": earthquake_report_status_row_data,
            }
            for table_name, row_data in table_name_row_data_dict.items():
                sql_query = insert_table_query(table_name)
                print(sql_query)
                print(row_data)
                cursor.execute(sql_query, row_data)
                connection.commit()


with DAG(
    dag_id="insert_earthquake_raw_data",
    default_args=default_args,
    start_date=datetime(2024, 4, 5, 0),
    schedule_interval="@daily",
) as dag:
    task_get_all_s3_objects = PythonOperator(
        task_id="task_get_all_s3_objects",
        python_callable=get_all_s3_objects,
    )
    task_insert_new_row_data = PythonOperator(
        task_id="task_insert_new_row_data",
        python_callable=insert_new_row_data,
    )
