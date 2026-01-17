from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat_ws, from_json, col
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType, DateType
)

# ======================================================
# Spark Session
# ======================================================
spark = (
    SparkSession.builder
        .appName("BronzeRetailIngestion")
        .master("local[*]")

        # Delta
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")

        # Numeric timeouts
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")

        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

bronze_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("age", IntegerType()),
    StructField("gender", StringType()),
    StructField("income_bracket", StringType()),
    StructField("loyalty_program", BooleanType()),
    StructField("membership_years", IntegerType()),
    StructField("churned", BooleanType()),
    StructField("marital_status", StringType()),
    StructField("number_of_children", IntegerType()),
    StructField("education_level", StringType()),
    StructField("occupation", StringType()),

    StructField("transaction_id", StringType()),
    StructField("transaction_date", DateType()),
    StructField("transaction_hour", IntegerType()),
    StructField("day_of_week", StringType()),
    StructField("week_of_year", IntegerType()),
    StructField("month_of_year", IntegerType()),

    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("product_category", StringType()),
    StructField("product_brand", StringType()),

    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("discount_applied", DoubleType()),
    StructField("payment_method", StringType()),
    StructField("store_location", StringType()),
    StructField("total_sales", DoubleType()),

    StructField("promotion_id", StringType()),
    StructField("promotion_type", StringType()),
    StructField("promotion_start_date", DateType()),
    StructField("promotion_end_date", DateType()),

    StructField("customer_city", StringType()),
    StructField("customer_state", StringType()),
    StructField("store_city", StringType()),
    StructField("store_state", StringType())
])

event_schema = StructType([
    StructField("source_system", StringType()),
    StructField("table_name", StringType()),
    StructField("operation", StringType()),
    StructField("event_time", StringType()),
    StructField("data", bronze_schema)
])


kafka_stream = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "retail_orders_raw")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
)

parsed_stream = (
    kafka_stream
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), event_schema).alias("event"))
        .select(
            # col("event.source_system"),
            # col("event.table_name"),
            # col("event.operation"),
            # col("event.event_time"),
            col("event.data.*")
        )
)

parsed_stream_with_event_id = parsed_stream.withColumn(
    "event_id",
    sha2(
        concat_ws(
            "||",
            col("customer_id"),
            col("transaction_id"),
            col("product_id"),
            col("transaction_date"),
            col("quantity"),
            col("unit_price")
        ),
        256
    )
)

def upsert_bronze(batch_df, batch_id):
    (
        batch_df
        .withColumnRenamed("event_id", "event_id")  # no-op, just clarity
        .createOrReplaceTempView("updates")
    )

    spark.sql("""
        MERGE INTO delta.`s3a://bronze/retail_raw` t
        USING updates u
        ON t.event_id = u.event_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

query = (
    parsed_stream_with_event_id.writeStream
        # .foreachBatch(upsert_bronze)
        .format("delta").outputMode("append")
        .option("checkpointLocation", "s3a://bronze/_checkpoints/retail_raw")
        .start("s3a://bronze/retail_raw")
)

query.awaitTermination()
