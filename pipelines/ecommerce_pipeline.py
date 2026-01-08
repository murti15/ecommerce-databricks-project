# ===============================
# STEP 0: Spark + Delta Setup
# ===============================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum
from pyspark.sql.types import *

# IMPORTANT: Create Spark Session (CI + Local)
spark = SparkSession.builder \
    .appName("Ecommerce Bronze Silver Gold Pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

print("Spark Version:", spark.version)

# ===============================
# STEP 1: SOURCE DATA
# ===============================

data = [
    (1, "C1", "P1", 2, 500, "2025-01-01", "PLACED"),
    (2, "C2", "P2", 1, -200, "2025-01-01", "PLACED"),
    (3, "C3", "P1", 1, 300, "2025-01-02", "CANCELLED"),
    (4, "C4", "P3", 3, 900, "2025-01-02", "PLACED")
]

schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("qty", IntegerType()),
    StructField("amount", IntegerType()),
    StructField("order_date", StringType()),
    StructField("status", StringType())
])

orders_df = spark.createDataFrame(data, schema)
orders_df.show()

# ===============================
# STEP 2: BRONZE
# ===============================

orders_df.write.format("delta").mode("overwrite").save("delta/bronze_orders")
bronze_df = spark.read.format("delta").load("delta/bronze_orders")
bronze_df.show()

# ===============================
# STEP 3: SILVER
# ===============================

silver_df = bronze_df \
    .filter(col("amount") > 0) \
    .filter(col("status") == "PLACED") \
    .withColumn("order_value", col("qty") * col("amount"))

silver_df.write.format("delta").mode("overwrite").save("delta/silver_orders")
silver_df.show()

# ===============================
# STEP 4: GOLD
# ===============================

gold_df = silver_df \
    .groupBy("order_date") \
    .agg(sum("order_value").alias("total_revenue"))

gold_df.write.format("delta").mode("overwrite").save("delta/gold_order_revenue")
gold_df.show()

spark.stop()
