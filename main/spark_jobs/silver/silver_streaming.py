from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col, lit
import random

spark = (
    SparkSession.builder
        .appName("SilverRetailProcessing")
        .master("local[2]")

        # Delta
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # Resource limits to prevent OOM
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")

        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")

        # Numeric timeouts ONLY
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("fs.s3a.connection.timeout", "60000")
        .config("fs.s3a.socket.timeout", "60000")
        .config("fs.s3a.connection.establish.timeout", "60000")

        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

bronze_df = spark.readStream.format("delta").load("s3a://bronze/retail_raw")

# -----------------------
# Clean transactions
# -----------------------
transactions_df = bronze_df.select(
    "event_id",
    "transaction_id",
    to_date("transaction_date").alias("transaction_date"),
    "transaction_hour",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "discount_applied",
    "payment_method",
    "store_location",
    "total_sales"
).dropDuplicates(["transaction_id"])

# -----------------------
# Clean customers
# -----------------------
customers_df = bronze_df.select(
    "customer_id",
    "age",
    "gender",
    "income_bracket",
    "loyalty_program",
    "membership_years",
    "churned",
    "customer_city",
    "customer_state"
).dropDuplicates(["customer_id"])


# -----------------------
# Write Silver
# -----------------------
transactions_df.writeStream.format("delta").outputMode("append") \
    .option(
        "checkpointLocation",
        "s3a://silver/_checkpoints/transactions"
    ) \
    .start("s3a://silver/transactions")

customers_df.writeStream.format("delta").outputMode("append") \
    .option(
        "checkpointLocation",
        "s3a://silver/_checkpoints/customers"
    ) \
    .start("s3a://silver/customers")


spark.streams.awaitAnyTermination()