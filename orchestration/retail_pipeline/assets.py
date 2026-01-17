import os
from dagster import asset
import subprocess

@asset(deps=["start_infrastructure"])
def bronze_ingestion(context):
    subprocess.Popen([
        "/Users/khoaiquin/Storage/spark-3.5.7-bin-hadoop3/bin/spark-submit",
        "--packages",
        ",".join([
            "io.delta:delta-spark_2.12:3.2.0",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.367",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7",
        ]),
        "/Users/khoaiquin/Storage/he_tinh_toan_phan_bo/Retail_Analysis_And_Management_Lakehouse/main/spark_jobs/bronze/bronze_streaming.py"
    ],
    # check=True,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL)

    context.log.info("Bronze Spark job started")
    return


@asset(deps=["start_infrastructure"])
def silver_processing(context):
    subprocess.Popen([
        "/Users/khoaiquin/Storage/spark-3.5.7-bin-hadoop3/bin/spark-submit",
        "--packages",
        ",".join([
            "io.delta:delta-spark_2.12:3.2.0",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.367",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7",
        ]),
        "/Users/khoaiquin/Storage/he_tinh_toan_phan_bo/Retail_Analysis_And_Management_Lakehouse/main/spark_jobs/silver/silver_streaming.py"
    ],
    # check=True,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL)

    context.log.info("Silver Spark job started")
    return


@asset(deps=["start_infrastructure"])
def gold_kpis(context):
    subprocess.Popen([
        "/Users/khoaiquin/Storage/spark-3.5.7-bin-hadoop3/bin/spark-submit",
        "--packages",
        ",".join([
            "io.delta:delta-spark_2.12:3.2.0",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.367",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7",
        ]),
        "/Users/khoaiquin/Storage/he_tinh_toan_phan_bo/Retail_Analysis_And_Management_Lakehouse/main/spark_jobs/gold/gold_streaming.py"
    ],
    # check=True,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL)

    context.log.info("Gold Spark job started")
    return
