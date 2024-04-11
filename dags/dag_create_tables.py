from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from utils.connection import s3_session, s3_client

default_args = {
    "owner":"Jess",
    "retries": 5,
    "retry_interval": timedelta(minutes=5)
}

with DAG(
    dag_id='create_tables',
    description='create all tables in postgreqsql',
    default_args=default_args,
    start_date=datetime(2024, 4, 7, 0),
    schedule_interval='@once'
) as dag:
    create_table_area = PostgresOperator(
        task_id='create_table_area',
        postgres_conn_id = 'airflow_postgres',
        sql='''
        CREATE TABLE IF NOT EXISTS area (
        area_id INTEGER,
        area_name VARCHAR(50) UNIQUE NOT NULL,
        insert_time TIME NOT NULL,
        PRIMARY KEY(area_id)
        )
        '''
    )

    create_table_county = PostgresOperator(
        task_id='create_table_county',
        postgres_conn_id = 'airflow_postgres',
        sql='''
        CREATE TABLE IF NOT EXISTS county (
        county_id INTEGER PRIMARY KEY,
        county_name VARCHAR(50) UNIQUE NOT NULL,
        insert_time TIME NOT NULL,
        PRIMARY KEY(county_id)
        )
        '''
    )

    create_table_earthquake_report = PostgresOperator(
        task_id='create_table_earthquake_report',
        postgres_conn_id = 'airflow_postgres',
        sql='''
        CREATE TABLE IF NOT EXISTS earthquake_report (
        earthquake_report_id INTEGER,
        report_type VARCHAR(50) NOT NULL,
        report_color VARCHAR(50) NOT NULL,
        report_content VARCHAR(250) NOT NULL,
        report_image_uri VARCHAR(100) NOT NULL,
        report_remark VARCHAR(250) NOT NULL,
        web_uri VARCHAR(50) NOT NULL,
        shakemap_image_uri VARCHAR(100) NOT NULL,
        insert_time TIME NOT NULL,
        PRIMARY KEY(earthquake_report_id)
        )
        '''
    )

    create_table_earthquake_info = PostgresOperator(
        task_id='create_table_earthquake_info',
        postgres_conn_id = 'airflow_postgres',
        sql='''
        CREATE TABLE IF NOT EXISTS earthquake_info (
        earthquake_info_id INTEGER,
        earthquake_report_id INTEGER NOT NULL,
        origin_time TIME NOT NULL,
        source VARCHAR(50) NOT NULL,
        focal_depth FLOAT NOT NULL,
        location VARCHAR(100) NOT NULL,
        epicenter_latitude FLOAT NOT NULL,
        epicenter_longtitude FLOAT NOT NULL,
        magnitude_type VARCHAR(100) NOT NULL,
        magnitude_value FLOAT NOT NULL,
        insert_time TIME NOT NULL,
        PRIMARY KEY(earthquake_info_id),
        CONSTRAINT fk_earthquake_info_earthquake_report
            FOREIGN KEY(earthquake_report_id)
            REFERENCES earthquake_report(earthquake_report_id)
        )
        '''
    )

    create_table_intensity = PostgresOperator(
        task_id='create_table_intensity',
        postgres_conn_id = 'airflow_postgres',
        sql='''
        CREATE TABLE IF NOT EXISTS intensity (
        intensity_id INTEGER NOT NULL,
        earthquake_report_id INTEGER NOT NULL,
        area_id INTEGER NOT NULL,
        county_id INTEGER NOT NULL,
        info_status VARCHAR(50) NOT NULL,
        area_intensity FLOAT NOT NULL,
        insert_time TIME NOT NULL,
        PRIMARY KEY(intensity_id),
        CONSTRAINT fk_intensity_earthquake_report
            FOREIGN KEY(earthquake_report_id)
            REFERENCES earthquake_report(earthquake_report_id),
        CONSTRAINT fk_intensity_area
            FOREIGN KEY(area_id)
            REFERENCES area(area_id),
        CONSTRAINT fk_intensity_county
            FOREIGN KEY(county_id)
            REFERENCES county(county_id)
        )
        '''
    )

    create_table_station_info = PostgresOperator(
        task_id='create_table_station_info',
        postgres_conn_id = 'airflow_postgres',
        sql='''
        CREATE TABLE IF NOT EXISTS station_info (
        station_info_id INTEGER NOT NULL,
        station_name VARCHAR(30) NOT NULL,
        station_id VARCHAR(10) NOT NULL,
        station_latitude FLOAT NOT NULL,
        station_longitude FLOAT NOT NULL,
        insert_time TIME NOT NULL,
        PRIMARY KEY(station_info_id)
        )
        '''
    )

    create_table_station_earthquake_data = PostgresOperator(
        task_id='create_table_station_earthquake_data',
        postgres_conn_id = 'airflow_postgres',
        sql='''
        CREATE TABLE IF NOT EXISTS station_earthquake_data (
        station_earthquake_data_id INTEGER NOT NULL,
        earthquake_report_id INTEGER NOT NULL,
        pga_unit VARCHAR(20) NOT NULL,
        pga_ew_component FLOAT NOT NULL,
        pga_ns_component FLOAT NOT NULL,
        pga_v_component FLOAT NOT NULL,
        pga_int_scale_value FLOAT NOT NULL,
        pgv_unit VARCHAR(20) NOT NULL,
        pgv_ew_component FLOAT NOT NULL,
        pgv_ns_component FLOAT NOT NULL,
        pgv_v_component FLOAT NOT NULL,
        pgv_int_scale_value FLOAT NOT NULL,
        station_info_id INTEGER NOT NULL,
        info_status VARCHAR(20) NOT NULL,
        back_azimuth FLOAT NOT NULL,
        epicenter_distance FLOAT NOT NULL,
        seismic_intensity VARCHAR(20) NOT NULL,
        wave_image_uri VARCHAR(100) NOT NULL,
        insert_time TIME NOT NULL,
        PRIMARY KEY(station_earthquake_data_id),
        CONSTRAINT fk_earthquake_station_data_earthquake_report
            FOREIGN KEY(earthquake_report_id)
            REFERENCES earthquake_report(earthquake_report_id),
        CONSTRAINT fk_earthquake_station_data_station_info
            FOREIGN KEY(station_info_id)
            REFERENCES station_info(station_info_id)
        )
        '''
    )


    create_table_area >> create_table_county >> create_table_earthquake_report >> create_table_earthquake_info >> create_table_intensity >> create_table_station_info >> create_table_station_earthquake_data