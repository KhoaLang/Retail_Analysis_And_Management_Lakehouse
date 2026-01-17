from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, col

spark = (
    SparkSession.builder
        .appName("clean-delta-bronze-batch")
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

        .getOrCreate()
)

bronze_path = "s3a://bronze/retail_raw"
silver_transaction_path = "s3a://silver/transactions"
silver_customer_path = "s3a://silver/customers"
gold_fact_sales_path = "s3a://gold/fact_sales"
gold_dim_customer_snapshot_path = "s3a://gold/dim_customer_snapshot"


for path in [
    bronze_path,
    silver_transaction_path,
    silver_customer_path
]:

    df = (
        spark.read
            .format("delta")
            .load(path)
    )

    # df = df.dropna(how="all")
    # df = df.filter(col("age").isNotNull())
    df = df.limit(0)

    df.write.format("delta") \
        .mode("overwrite") \
        .save(bronze_path)

# df.printSchema()
    df.show(2, truncate=False)#.sort(desc("event_time"))
# .filter(col("price_usd") < 0)

spark.stop()
