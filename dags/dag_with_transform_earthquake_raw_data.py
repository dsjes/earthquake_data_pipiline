from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

sys.path.append(os.path.abspath("/opt/airflow/config"))
from config.connection import s3_session, s3_client, postgres_conn
from datetime import datetime, timedelta
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
        return result


def collect_earthquake_report_row_data(data):
    # collect earthquake_report row data from fetch_data_from_s3 output
    earthquake_number = data["EarthquakeNo"]
    report_type = data["ReportType"]
    report_color = data["ReportColor"]
    report_content = data["ReportContent"]
    report_image_uri = data["ReportImageURI"]
    report_remark = data["ReportRemark"]
    web_uri = data["Web"]
    shakemap_image_uri = data["ShakemapImageURI"]
    return [
        (
            earthquake_number,
            report_type,
            report_color,
            report_content,
            report_image_uri,
            report_remark,
            web_uri,
            shakemap_image_uri,
        )
    ]


def collect_earthquake_info_row_data(data):
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
    return [
        (
            earthquake_number,
            origin_time,
            source,
            focal_depth,
            location,
            epicenter_latitude,
            epicenter_longtitude,
            magnitude_type,
            magnitude_value,
        )
    ]


def collect_earthquake_report_status_row_data(bucket_object):
    print(f"bucket_object:{bucket_object}")
    # collect earthquake_report_status row data from fetch_data_from_s3 output
    match = re.search(r"\d+", bucket_object)
    if match:
        return [(match.group(), "0")]


def collect_area_row_data(data):
    # collect area row data from fetch_data_from_s3 output
    shaking_area_list = data["Intensity"]["ShakingArea"]
    area_list = []
    for shaking_area in shaking_area_list:
        if "最大震度" not in shaking_area["AreaDesc"]:
            area_list.append((shaking_area["AreaDesc"],))
    return area_list


def collect_county_row_data(data):
    # collect county row data from fetch_data_from_s3 output
    shaking_area_list = data["Intensity"]["ShakingArea"]
    county_list = []
    for shaking_area in shaking_area_list:
        if "、" in shaking_area["CountyName"]:
            multiple_county_names = shaking_area["CountyName"].split("、")
            for county_name in multiple_county_names:
                county_list.append((county_name,))
        else:
            county_list.append((shaking_area["CountyName"],))
    return list(set(county_list))


def get_area_id(area_name):
    print("========")
    print(area_name)
    with postgres_conn() as (connection, cursor):
        sql_query = "select area_id from area where area_name = %s"
        cursor.execute(sql_query, (area_name,))
        result = cursor.fetchone()[0]
        return result


def get_county_id(county_name):
    print("========")
    print(county_name)
    with postgres_conn() as (connection, cursor):
        sql_query = "select county_id from county where county_name = %s"
        cursor.execute(sql_query, (county_name,))
        result = cursor.fetchone()[0]
        return result


def collect_intensity_row_data(data):
    # collect intensity row data from fetch_data_from_s3 output
    intensity_row_data_list = []
    shaking_area_list = data["Intensity"]["ShakingArea"]
    for shaking_area in shaking_area_list:
        if "最大震度" not in shaking_area["AreaDesc"]:
            earthquake_number = data["EarthquakeNo"]
            area_name = shaking_area["AreaDesc"]
            area_id = get_area_id(area_name)
            county_name = shaking_area["CountyName"]
            county_id = get_county_id(county_name)
            info_status = shaking_area["InfoStatus"]
            area_intensity_string = shaking_area["AreaIntensity"]
            area_intensity = int(area_intensity_string.replace("級", ""))
            intensity_row_data_list.append(
                (
                    earthquake_number,
                    area_id,
                    county_id,
                    info_status,
                    area_intensity,
                )
            )
    print(intensity_row_data_list)
    return intensity_row_data_list


def collect_station_info_row_data(data):
    # collect intensity row data from fetch_data_from_s3 output
    station_info_row_data_list = []
    shaking_area_list = data["Intensity"]["ShakingArea"]
    for shaking_area in shaking_area_list:
        eq_station_list = shaking_area["EqStation"]
        for eq_station in eq_station_list:
            station_name = eq_station["StationName"]
            station_id = eq_station["StationID"]
            station_latitude = eq_station["StationLatitude"]
            station_longitude = eq_station["StationLongitude"]
            station_info_row_data_list.append(
                (
                    station_name,
                    station_id,
                    station_latitude,
                    station_longitude,
                )
            )
    print(station_info_row_data_list)
    return station_info_row_data_list


def get_station_info_id(station_name):
    with postgres_conn() as (connection, cursor):
        print(station_name)
        sql_query = "select station_info_id from station_info where station_name = %s"
        cursor.execute(sql_query, (station_name,))
        result = cursor.fetchone()[0]
        return result


def collect_station_earthquake_data_row_data(data):
    # collect station_earthquake_data row data from fetch_data_from_s3 output
    station_earthquake_data_row_data_list = []
    shaking_area_list = data["Intensity"]["ShakingArea"]
    for shaking_area in shaking_area_list:
        eq_station_list = shaking_area["EqStation"]
        for eq_station in eq_station_list:
            if len(eq_station) != 0 and "pga" in eq_station and "pgv" in eq_station:
                earthquake_number = data["EarthquakeNo"]
                pga_unit = eq_station["pga"]["unit"]
                pga_ew_component = eq_station["pga"]["EWComponent"]
                pga_ns_component = eq_station["pga"]["NSComponent"]
                pga_v_component = eq_station["pga"]["VComponent"]
                pga_int_scale_value = eq_station["pga"]["IntScaleValue"]
                pgv_unit = eq_station["pgv"]["unit"]
                pgv_ew_component = eq_station["pgv"]["EWComponent"]
                pgv_ns_component = eq_station["pgv"]["NSComponent"]
                pgv_v_component = eq_station["pgv"]["VComponent"]
                pgv_int_scale_value = eq_station["pgv"]["IntScaleValue"]
                station_name = eq_station["StationName"]
                station_info_id = get_station_info_id(station_name)
                info_status = eq_station["InfoStatus"]
                back_azimuth = eq_station["BackAzimuth"]
                epicenter_distance = eq_station["EpicenterDistance"]
                seismic_intensity = eq_station["SeismicIntensity"]
                wave_image_uri = eq_station["WaveImageURI"]
                station_earthquake_data_row_data_list.append(
                    (
                        earthquake_number,
                        pga_unit,
                        pga_ew_component,
                        pga_ns_component,
                        pga_v_component,
                        pga_int_scale_value,
                        pgv_unit,
                        pgv_ew_component,
                        pgv_ns_component,
                        pgv_v_component,
                        pgv_int_scale_value,
                        station_info_id,
                        info_status,
                        back_azimuth,
                        epicenter_distance,
                        seismic_intensity,
                        wave_image_uri,
                    )
                )
    return station_earthquake_data_row_data_list


def get_all_columns(table_name):
    with postgres_conn() as (connection, cursor):
        sql_query = """
            SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name = %s
            """
        cursor.execute(sql_query, (table_name,))
        columns_list = [row[0] for row in cursor.fetchall()][1:-1]
        return columns_list


def insert_table_query(table_name):
    columns_list = get_all_columns(table_name)
    columns_string = ", ".join(columns_list)
    placeholders = ", ".join(["%s"] * len(columns_list))
    sql_query = f"""INSERT INTO {table_name} ({columns_string}) 
    VALUES ({placeholders})
    ON CONFLICT DO NOTHING"""
    return sql_query


def insert_earthquake_report_row_data(data):
    with postgres_conn() as (connection, cursor):
        earthquake_report_row_data = collect_earthquake_report_row_data(data)
        print(earthquake_report_row_data)
        sql_query = insert_table_query("earthquake_report")
        cursor.execute(sql_query, earthquake_report_row_data[0])
        connection.commit()


def insert_earthquake_info_row_data(data):
    with postgres_conn() as (connection, cursor):
        earthquake_info_row_data = collect_earthquake_info_row_data(data)
        print(earthquake_info_row_data)
        sql_query = insert_table_query("earthquake_info")
        cursor.execute(sql_query, earthquake_info_row_data[0])
        connection.commit()


def insert_earthquake_report_status_row_data(bucket_object):
    with postgres_conn() as (connection, cursor):
        earthquake_report_status_row_data = collect_earthquake_report_status_row_data(
            bucket_object
        )
        print(earthquake_report_status_row_data)
        sql_query = insert_table_query("earthquake_report_status")
        if len(earthquake_report_status_row_data) > 1:
            cursor.executemany(sql_query, earthquake_report_status_row_data)
        else:
            cursor.execute(sql_query, earthquake_report_status_row_data[0])
        connection.commit()


def insert_area_row_data(data):
    with postgres_conn() as (connection, cursor):
        area_row_data = collect_area_row_data(data)
        print(area_row_data)
        sql_query = insert_table_query("area")
        if len(area_row_data) > 1:
            cursor.executemany(sql_query, area_row_data)
        else:
            cursor.execute(sql_query, area_row_data[0])
        connection.commit()


def insert_county_row_data(data):
    with postgres_conn() as (connection, cursor):
        county_row_data = collect_county_row_data(data)
        print(county_row_data)
        sql_query = insert_table_query("county")
        if len(county_row_data) > 1:
            cursor.executemany(sql_query, county_row_data)
        else:
            cursor.execute(sql_query, county_row_data[0])
        connection.commit()


def insert_intensity_row_data(data):
    with postgres_conn() as (connection, cursor):
        intensity_row_data = collect_intensity_row_data(data)
        print(intensity_row_data)
        sql_query = insert_table_query("intensity")
        if len(intensity_row_data) > 1:
            cursor.executemany(sql_query, intensity_row_data)
        else:
            cursor.execute(sql_query, intensity_row_data[0])
        connection.commit()


def insert_station_info_row_data(data):
    with postgres_conn() as (connection, cursor):
        station_info_row_data = collect_station_info_row_data(data)
        print(station_info_row_data)
        sql_query = insert_table_query("station_info")
        if len(station_info_row_data) > 1:
            cursor.executemany(sql_query, station_info_row_data)
        else:
            cursor.execute(sql_query, station_info_row_data[0])
        connection.commit()


def insert_station_earthquake_data_row_data(data):
    with postgres_conn() as (connection, cursor):
        station_earthquake_data_row_data = collect_station_earthquake_data_row_data(
            data
        )
        print(station_earthquake_data_row_data)
        sql_query = insert_table_query("station_earthquake_data")
        if len(station_earthquake_data_row_data) > 1:
            cursor.executemany(sql_query, station_earthquake_data_row_data)
        else:
            cursor.execute(sql_query, station_earthquake_data_row_data[0])
        connection.commit()


def insert_all_data():
    bucket_object_list = get_all_s3_objects()
    for bucket_object in bucket_object_list:
        data = fetch_data_from_s3(key=f"earthquake_source_data/{bucket_object}")
        insert_earthquake_report_row_data(data)
        insert_earthquake_info_row_data(data)
        insert_earthquake_report_status_row_data(bucket_object)
        insert_area_row_data(data)
        insert_county_row_data(data)
        insert_intensity_row_data(data)
        insert_station_info_row_data(data)
        insert_station_earthquake_data_row_data(data)


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

    task_insert_all_data = PythonOperator(
        task_id="task_insert_all_data",
        python_callable=insert_all_data,
    )
    task_get_all_s3_objects >> task_insert_all_data
    # task_insert_earthquake_report_row_data = PythonOperator(
    #     task_id="task_insert_earthquake_report_row_data",
    #     python_callable=insert_earthquake_report_row_data,
    # )
    # task_insert_earthquake_info_row_data = PythonOperator(
    #     task_id="task_insert_earthquake_info_row_data",
    #     python_callable=insert_earthquake_info_row_data,
    # )

    # task_insert_earthquake_report_status_row_data = PythonOperator(
    #     task_id="task_insert_earthquake_report_status_row_data",
    #     python_callable=insert_earthquake_report_status_row_data,
    # )

    # task_insert_area_row_data = PythonOperator(
    #     task_id="task_insert_area_row_data",
    #     python_callable=insert_area_row_data,
    # )

    # task_insert_county_row_data = PythonOperator(
    #     task_id="task_insert_county_row_data",
    #     python_callable=insert_county_row_data,
    # )

    # task_insert_intensity_row_data = PythonOperator(
    #     task_id="task_insert_intensity_row_data",
    #     python_callable=insert_intensity_row_data,
    # )

    # task_insert_station_info_row_data = PythonOperator(
    #     task_id="task_insert_station_info_row_data",
    #     python_callable=insert_station_info_row_data,
    # )

    # task_insert_station_earthquake_data_row_data = PythonOperator(
    #     task_id="task_insert_station_earthquake_data_row_data",
    #     python_callable=insert_station_earthquake_data_row_data,
    # )

    # (
    #     task_get_all_s3_objects
    #     >> task_insert_earthquake_report_row_data
    #     >> task_insert_earthquake_info_row_data
    #     >> task_insert_earthquake_report_status_row_data
    #     >> task_insert_area_row_data
    #     >> task_insert_county_row_data
    #     >> task_insert_intensity_row_data
    #     >> task_insert_station_info_row_data
    #     >> task_insert_station_earthquake_data_row_data
    # )
