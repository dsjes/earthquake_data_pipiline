from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

with DAG(
    "AY_data_pipleine",
    start_date=datetime(2023, 5, 27),
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
) as dag:
    run_spark_job = SparkSubmitOperator(
        task_id="run_spark_job",
        application="/opt/airflow/dags/pyspark_test.py",
        conn_id="spark_conn",
        executor_memory="2g",
        total_executor_cores=2,
        verbose=True,
    )
    run_spark_job
