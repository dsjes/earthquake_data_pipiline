from pyspark.sql import SparkSession
import os


def main():
    spark = spark = (
        SparkSession.builder.master("local[*]").appName("My App").getOrCreate()
    )

    # jdbc_url = f"jdbc:postgresql://{os.environ['PG_HOST']}:{os.environ['PG_PORT']}/{os.environ['PG_DB']}?user={os.environ['PG_USER']}&password={os.environ['PG_PASSWORD']}"
    jdbc_url = f"jdbc:postgresql://postgres:5432/postgres?user=airflow&password=airflow"
    table = "area"

    df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    df.show()

    spark.stop()


if __name__ == "__main__":
    main()
