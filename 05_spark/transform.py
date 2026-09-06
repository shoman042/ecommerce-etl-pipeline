"""
Ecommerce ETL - Spark Transformation Layer
============================================
Reads raw data from HDFS staging zone (loaded by NiFi from MariaDB),
cleans it, applies business transformations, and writes analytics-ready
tables as Parquet to HDFS analytics zone.

Run with:
    spark-submit --master local[*] transform.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, StringType,
    DoubleType, IntegerType, DateType
)

# ============================================================
# CONFIG
# ============================================================

RAW_BASE = "/staging_zone/ecommerce"
CLEAN_BASE = "/clean_zone/ecommerce"
ANALYTICS_BASE = "/analytics_zone/ecommerce"

spark = (
    SparkSession.builder
    .appName("EcommerceETL")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ============================================================
# 1. READ RAW DATA (explicit schemas - files have NO header)
# ============================================================

customers_schema = StructType([
    StructField("customer_id", LongType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("signup_date", StringType(), True),
])

products_schema = StructType([
    StructField("product_id", LongType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("stock_quantity", IntegerType(), True),
])

orders_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("shipping_country", StringType(), True),
])

order_items_schema = StructType([
    StructField("order_item_id", LongType(), True),
    StructField("order_id", LongType(), True),
    StructField("product_id", LongType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
])

payments_schema = StructType([
    StructField("payment_id", LongType(), True),
    StructField("order_id", LongType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("payment_date", StringType(), True),
    StructField("amount", DoubleType(), True),
])


def read_raw(name, schema):
    return (
        spark.read
        .option("header", "false")
        .option("quote", '"')
        .option("nullValue", "")
        .schema(schema)
        .csv(f"{RAW_BASE}/{name}")
    )


print("Reading raw data from HDFS...")
customers_raw = read_raw("customers", customers_schema)
products_raw = read_raw("products", products_schema)
orders_raw = read_raw("orders", orders_schema)
order_items_raw = read_raw("order_items", order_items_schema)
payments_raw = read_raw("payments", payments_schema)

raw_counts = {
    "customers": customers_raw.count(),
    "products": products_raw.count(),
    "orders": orders_raw.count(),
    "order_items": order_items_raw.count(),
    "payments": payments_raw.count(),
}
print("Raw row counts:", raw_counts)

# ============================================================
# 2. DATA CLEANING
# ============================================================

# NiFi CSVRecordSetWriter writes timestamps like "2025-03-16 09:14:38.0"
# (single trailing fractional-second digit) -> handle explicitly.
TS_FORMAT = "yyyy-MM-dd HH:mm:ss.S"


def clean_customers(df):
    df = df.dropDuplicates(["customer_id"])
    df = df.filter(F.col("customer_id").isNotNull())

    df = df.withColumn("first_name", F.trim(F.col("first_name")))
    df = df.withColumn("last_name", F.trim(F.col("last_name")))
    df = df.withColumn("email", F.lower(F.trim(F.col("email"))))
    df = df.withColumn("country", F.trim(F.col("country")))
    df = df.withColumn("city", F.trim(F.col("city")))

    # convert to real DateType and validate: not null, not in the future
    df = df.withColumn("signup_date", F.to_date(F.col("signup_date"), "yyyy-MM-dd"))
    df = df.filter(
        F.col("signup_date").isNotNull() &
        (F.col("signup_date") <= F.current_date())
    )

    # fill any remaining nulls in non-critical text fields
    df = df.fillna({"country": "Unknown", "city": "Unknown"})

    return df


def clean_products(df):
    df = df.dropDuplicates(["product_id"])
    df = df.filter(F.col("product_id").isNotNull())

    df = df.withColumn("product_name", F.trim(F.col("product_name")))
    df = df.withColumn("category", F.trim(F.col("category")))
    df = df.fillna({"category": "Uncategorized"})

    # validate price: must be non-null and > 0
    df = df.filter(F.col("price").isNotNull() & (F.col("price") > 0))

    # stock_quantity can't be negative
    df = df.withColumn(
        "stock_quantity",
        F.when(F.col("stock_quantity") < 0, 0).otherwise(F.col("stock_quantity"))
    )
    df = df.fillna({"stock_quantity": 0})

    return df


def clean_orders(df):
    df = df.dropDuplicates(["order_id"])
    df = df.filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull())

    df = df.withColumn(
        "order_date", F.to_timestamp(F.col("order_date"), TS_FORMAT)
    )
    df = df.filter(
        F.col("order_date").isNotNull() &
        (F.col("order_date") <= F.current_timestamp())
    )

    df = df.withColumn("status", F.lower(F.trim(F.col("status"))))
    df = df.fillna({"status": "unknown", "shipping_country": "Unknown"})

    return df


def clean_order_items(df):
    df = df.dropDuplicates(["order_item_id"])
    df = df.filter(
        F.col("order_item_id").isNotNull() &
        F.col("order_id").isNotNull() &
        F.col("product_id").isNotNull()
    )

    # validate quantity and unit_price
    df = df.filter(
        F.col("quantity").isNotNull() & (F.col("quantity") > 0) &
        F.col("unit_price").isNotNull() & (F.col("unit_price") > 0)
    )

    return df


def clean_payments(df):
    df = df.dropDuplicates(["payment_id"])
    df = df.filter(F.col("payment_id").isNotNull() & F.col("order_id").isNotNull())

    df = df.withColumn(
        "payment_date", F.to_timestamp(F.col("payment_date"), TS_FORMAT)
    )
    df = df.withColumn("payment_status", F.lower(F.trim(F.col("payment_status"))))
    df = df.fillna({"payment_status": "unknown"})

    return df


print("Cleaning data...")
customers = clean_customers(customers_raw)
products = clean_products(products_raw)
orders = clean_orders(orders_raw)
order_items = clean_order_items(order_items_raw)
payments = clean_payments(payments_raw)

clean_counts = {
    "customers": customers.count(),
    "products": products.count(),
    "orders": orders.count(),
    "order_items": order_items.count(),
    "payments": payments.count(),
}
print("Clean row counts:", clean_counts)
print("Rows dropped during cleaning:", {
    k: raw_counts[k] - clean_counts[k] for k in raw_counts
})

# persist cleaned layer too (useful for debugging / re-runs)
customers.write.mode("overwrite").parquet(f"{CLEAN_BASE}/customers")
products.write.mode("overwrite").parquet(f"{CLEAN_BASE}/products")
orders.write.mode("overwrite").parquet(f"{CLEAN_BASE}/orders")
order_items.write.mode("overwrite").parquet(f"{CLEAN_BASE}/order_items")
payments.write.mode("overwrite").parquet(f"{CLEAN_BASE}/payments")

# ============================================================
# 3. BUSINESS TRANSFORMATIONS - JOIN
# ============================================================

print("Joining orders + order_items + products + customers...")

sales = (
    order_items
    .join(orders, on="order_id", how="inner")
    .join(products, on="product_id", how="inner")
    .join(customers, on="customer_id", how="inner")
    .withColumn("total_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
)

sales.cache()
print("Sales dataset row count:", sales.count())

# ---- Overall summary metrics (printed for the record / validation) ----

summary = sales.agg(
    F.round(F.sum("total_amount"), 2).alias("total_sales"),
    F.sum("quantity").alias("total_quantity_sold"),
    F.countDistinct("order_id").alias("number_of_orders"),
    F.countDistinct("customer_id").alias("number_of_customers"),
).withColumn(
    "average_order_value",
    F.round(F.col("total_sales") / F.col("number_of_orders"), 2)
)

print("Overall business summary:")
summary.show(truncate=False)

sales_by_country = (
    sales.groupBy("country")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("total_sales"),
        F.sum("quantity").alias("total_quantity")
    )
    .orderBy(F.desc("total_sales"))
)

sales_by_category = (
    sales.groupBy("category")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("total_sales"),
        F.sum("quantity").alias("total_quantity")
    )
    .orderBy(F.desc("total_sales"))
)

print("Sales by country:")
sales_by_country.show(truncate=False)
print("Sales by category:")
sales_by_category.show(truncate=False)

# ============================================================
# 4. ANALYTICS TABLES
# ============================================================

# ---- fact_sales ----
fact_sales = sales.select(
    F.col("order_id"),
    F.col("customer_id"),
    F.col("product_id"),
    F.col("order_date"),
    F.col("quantity"),
    F.col("unit_price"),
    F.col("total_amount"),
    F.col("category"),
    F.col("country"),
    F.col("status"),
)

# ---- daily_sales ----
daily_sales = (
    sales
    .withColumn("year", F.year("order_date"))
    .withColumn("month", F.month("order_date"))
    .withColumn("day", F.dayofmonth("order_date"))
    .groupBy("year", "month", "day")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_quantity"),
        F.round(F.sum("total_amount"), 2).alias("total_sales"),
    )
    .withColumn(
        "average_order_value",
        F.round(F.col("total_sales") / F.col("total_orders"), 2)
    )
    .orderBy("year", "month", "day")
)

# ---- customer_sales ----
customer_sales = (
    sales
    .withColumn("customer_name", F.concat_ws(" ", F.col("first_name"), F.col("last_name")))
    .groupBy("customer_id", "customer_name", "country")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_quantity"),
        F.round(F.sum("total_amount"), 2).alias("total_spending"),
    )
    .orderBy(F.desc("total_spending"))
)

# ---- product_sales ----
product_sales = (
    sales
    .groupBy("product_id", "product_name", "category")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.round(F.sum("total_amount"), 2).alias("total_sales"),
    )
    .orderBy(F.desc("total_sales"))
)

# ---- monthly / yearly rollups (extra, requested in business requirements) ----
monthly_sales = (
    sales
    .withColumn("year", F.year("order_date"))
    .withColumn("month", F.month("order_date"))
    .groupBy("year", "month")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_quantity"),
        F.round(F.sum("total_amount"), 2).alias("total_sales"),
    )
    .orderBy("year", "month")
)

yearly_sales = (
    sales
    .withColumn("year", F.year("order_date"))
    .groupBy("year")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_quantity"),
        F.round(F.sum("total_amount"), 2).alias("total_sales"),
    )
    .orderBy("year")
)

# ============================================================
# 5. WRITE ANALYTICS TABLES TO HDFS AS PARQUET
# ============================================================

print("Writing analytics tables as Parquet to HDFS...")

fact_sales.write.mode("overwrite").partitionBy("country").parquet(
    f"{ANALYTICS_BASE}/fact_sales"
)
daily_sales.write.mode("overwrite").parquet(f"{ANALYTICS_BASE}/daily_sales")
customer_sales.write.mode("overwrite").parquet(f"{ANALYTICS_BASE}/customer_sales")
product_sales.write.mode("overwrite").parquet(f"{ANALYTICS_BASE}/product_sales")
monthly_sales.write.mode("overwrite").parquet(f"{ANALYTICS_BASE}/monthly_sales")
yearly_sales.write.mode("overwrite").parquet(f"{ANALYTICS_BASE}/yearly_sales")
sales_by_country.write.mode("overwrite").parquet(f"{ANALYTICS_BASE}/sales_by_country")
sales_by_category.write.mode("overwrite").parquet(f"{ANALYTICS_BASE}/sales_by_category")

print("====================================")
print("SPARK TRANSFORMATION COMPLETED")
print("====================================")

spark.stop()
