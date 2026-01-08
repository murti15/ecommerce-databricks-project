# =========================================================
# E-COMMERCE DATA PIPELINE
# Bronze → Silver → Gold Architecture
# Technology: PySpark + Delta Lake (Azure Databricks style)
# =========================================================


# =========================================================
# STEP 0: SOURCE DATA (SIMULATED INGESTION)
# In real projects: ADLS / Blob / EventHub / Kafka
# =========================================================

from pyspark.sql.types import *
from pyspark.sql.functions import *

# Sample order data
data = [
    (1, "C1", "P1", 2, 500, "2025-01-01", "PLACED"),
    (2, "C2", "P2", 1, -200, "2025-01-01", "PLACED"),     # invalid amount
    (3, "C3", "P1", 1, 300, "2025-01-02", "CANCELLED"),  # cancelled order
    (4, "C4", "P3", 3, 900, "2025-01-02", "PLACED")
]

# Define schema
schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("qty", IntegerType()),
    StructField("amount", IntegerType()),
    StructField("order_date", StringType()),
    StructField("status", StringType())
])

# Create Spark DataFrame
orders_df = spark.createDataFrame(data, schema)

print("STEP 0: SOURCE DATA")
display(orders_df)


# =========================================================
# STEP 1: BRONZE LAYER (RAW DATA)
# Purpose: Store raw, immutable data for audit & replay
# =========================================================

orders_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_orders")

print("STEP 1: BRONZE TABLE CREATED")
spark.sql("SELECT * FROM bronze_orders").show()


# =========================================================
# STEP 2: SILVER LAYER (DATA QUALITY + order_value)
# Rules:
#   - amount must be > 0
#   - status must be PLACED
# Derived Column:
#   - order_value = qty * amount
# =========================================================

silver_df = spark.table("bronze_orders") \
    .filter("amount > 0") \
    .filter("status = 'PLACED'") \
    .withColumn("order_value", col("qty") * col("amount"))

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_orders")

print("STEP 2: SILVER TABLE CREATED (DATA QUALITY + order_value)")
spark.sql("SELECT * FROM silver_orders").show()


# =========================================================
# STEP 3: GOLD LAYER (BUSINESS AGGREGATION)
# Business Metric:
#   - Total revenue per day
# =========================================================

gold_df = spark.table("silver_orders") \
    .groupBy("order_date") \
    .agg(sum("order_value").alias("total_revenue"))

gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_order_revenue")

print("STEP 3: GOLD TABLE CREATED (BUSINESS METRICS)")
spark.sql("SELECT * FROM gold_order_revenue").show()


# =========================================================
# END OF PIPELINE
# =========================================================
print("✅ Pipeline Execution Complete!")
