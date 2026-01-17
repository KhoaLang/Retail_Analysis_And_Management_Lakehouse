from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, count, month, window, date_format, row_number, col
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
        .appName("GoldRetailKPI")
        .master("local[2]")

        # Delta
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

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

transactions = spark.readStream.format("delta").load("s3a://silver/transactions") \
    .withColumn("transaction_date", col("transaction_date").cast("timestamp"))
customers = spark.readStream.format("delta").load("s3a://silver/customers")


gold_sales = (
    transactions
        .select(
            "transaction_date",
            "transaction_hour",
            "product_id",
            "customer_id",
            "quantity",
            "unit_price",
            "total_sales"
        )
)

gold_customers = customers.select(
    "customer_id",
    "age",
    "gender",
    "income_bracket",
    "loyalty_program",
    "membership_years",
    "churned",
    "customer_city",
    "customer_state"
)

gold_sales.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://gold/_checkpoints/fact_sales") \
    .start("s3a://gold/fact_sales")

gold_customers.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://gold/_checkpoints/dim_customer_snapshot") \
    .start("s3a://gold/dim_customer_snapshot")

spark.streams.awaitAnyTermination()
