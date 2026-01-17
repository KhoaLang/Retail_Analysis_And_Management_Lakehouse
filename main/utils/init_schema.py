from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType, DateType
)

spark = (
    SparkSession.builder
        .appName("InitLakehouseSchemas")
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

        # Numeric timeouts ONLY
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60000")
        .config("fs.s3a.connection.timeout", "60000")
        .config("fs.s3a.socket.timeout", "60000")
        .config("fs.s3a.connection.establish.timeout", "60000")

        .getOrCreate()
)


# # =========================================================
# # 🥉 BRONZE SCHEMA (Raw, Wide Table)
# # =========================================================

# bronze_schema = StructType([
#     StructField("event_id", StringType()),
#     StructField("customer_id", StringType()),
#     StructField("age", IntegerType()),
#     StructField("gender", StringType()),
#     StructField("income_bracket", StringType()),
#     StructField("loyalty_program", BooleanType()),
#     StructField("membership_years", IntegerType()),
#     StructField("churned", BooleanType()),
#     StructField("marital_status", StringType()),
#     StructField("number_of_children", IntegerType()),
#     StructField("education_level", StringType()),
#     StructField("occupation", StringType()),

#     StructField("transaction_id", StringType()),
#     StructField("transaction_date", DateType()),
#     StructField("transaction_hour", IntegerType()),
#     StructField("day_of_week", StringType()),
#     StructField("week_of_year", IntegerType()),
#     StructField("month_of_year", IntegerType()),

#     StructField("product_id", StringType()),
#     StructField("product_name", StringType()),
#     StructField("product_category", StringType()),
#     StructField("product_brand", StringType()),

#     StructField("quantity", IntegerType()),
#     StructField("unit_price", DoubleType()),
#     StructField("discount_applied", DoubleType()),
#     StructField("payment_method", StringType()),
#     StructField("store_location", StringType()),
#     StructField("total_sales", DoubleType()),

#     StructField("promotion_id", StringType()),
#     StructField("promotion_type", StringType()),
#     StructField("promotion_start_date", DateType()),
#     StructField("promotion_end_date", DateType()),

#     StructField("customer_city", StringType()),
#     StructField("customer_state", StringType()),
#     StructField("store_city", StringType()),
#     StructField("store_state", StringType())
# ])

# empty_bronze_df = spark.createDataFrame([], bronze_schema)

# empty_bronze_df.write.format("delta") \
#     .mode("overwrite") \
#     .save("s3a://bronze/retail_raw")

# =========================================================
# 🥈 SILVER SCHEMAS (Normalized)
# =========================================================

# -------- Transactions --------
transactions_schema = StructType([
    StructField("event_id", StringType()),
    StructField("transaction_id", StringType()),
    StructField("transaction_date", DateType()),
    StructField("transaction_hour", IntegerType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("discount_applied", DoubleType()),
    StructField("payment_method", StringType()),
    StructField("store_location", StringType()),
    StructField("total_sales", DoubleType())
])

spark.createDataFrame([], transactions_schema) \
    .write.format("delta").mode("overwrite") \
    .save("s3a://silver/transactions")

# -------- Customers --------
customers_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("age", IntegerType()),
    StructField("gender", StringType()),
    StructField("income_bracket", StringType()),
    StructField("loyalty_program", BooleanType()),
    StructField("membership_years", IntegerType()),
    StructField("churned", BooleanType()),
    StructField("customer_city", StringType()),
    StructField("customer_state", StringType())
])

spark.createDataFrame([], customers_schema) \
    .write.format("delta").mode("overwrite") \
    .save("s3a://silver/customers")

# # -------- Products --------
# products_schema = StructType([
#     StructField("product_id", StringType()),
#     StructField("product_name", StringType()),
#     StructField("product_category", StringType()),
#     StructField("product_brand", StringType()),
#     StructField("unit_price", DoubleType()),
#     StructField("product_rating", DoubleType()),
#     StructField("product_stock", IntegerType())
# ])

# spark.createDataFrame([], products_schema) \
#     .write.format("delta").mode("overwrite") \
#     .save("s3a://silver/products") \

# # =========================================================
# # 🥇 GOLD SCHEMAS (KPIs)
# # =========================================================

# # -------- Monthly Revenue KPI --------
# monthly_revenue_schema = StructType([
#     StructField("month", IntegerType()),
#     StructField("total_revenue", DoubleType()),
#     StructField("total_transactions", IntegerType())
# ])

# spark.createDataFrame([], monthly_revenue_schema) \
#     .write.format("delta").mode("overwrite") \
#     .save("s3a://gold/monthly_revenue")

# # -------- Customer Churn KPI --------
# customer_churn_schema = StructType([
#     StructField("churned", BooleanType()),
#     StructField("customer_count", IntegerType())
# ])

# spark.createDataFrame([], customer_churn_schema) \
#     .write.format("delta").mode("overwrite") \
#     .save("s3a://gold/customer_churn")

print("✅ Lakehouse schemas initialized successfully")
