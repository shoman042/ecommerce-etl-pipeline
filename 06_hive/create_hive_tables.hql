-- ============================================================
-- Ecommerce Data Warehouse - Hive DDL
-- Run with: hive -f create_hive_tables.hql
-- ============================================================

CREATE DATABASE IF NOT EXISTS ecommerce_dw;

USE ecommerce_dw;

-- ------------------------------------------------------------
-- fact_sales  (partitioned by country, written that way by Spark)
-- ------------------------------------------------------------
DROP TABLE IF EXISTS fact_sales;

CREATE EXTERNAL TABLE fact_sales (
    order_id      BIGINT,
    customer_id   BIGINT,
    product_id    BIGINT,
    order_date    TIMESTAMP,
    quantity      INT,
    unit_price    DOUBLE,
    total_amount  DOUBLE,
    category      STRING,
    status        STRING
)
PARTITIONED BY (country STRING)
STORED AS PARQUET
LOCATION '/analytics_zone/ecommerce/fact_sales';

MSCK REPAIR TABLE fact_sales;

-- ------------------------------------------------------------
-- daily_sales
-- ------------------------------------------------------------
DROP TABLE IF EXISTS daily_sales;

CREATE EXTERNAL TABLE daily_sales (
    year                 INT,
    month                INT,
    day                  INT,
    total_orders         BIGINT,
    total_quantity       BIGINT,
    total_sales          DOUBLE,
    average_order_value  DOUBLE
)
STORED AS PARQUET
LOCATION '/analytics_zone/ecommerce/daily_sales';

-- ------------------------------------------------------------
-- customer_sales
-- ------------------------------------------------------------
DROP TABLE IF EXISTS customer_sales;

CREATE EXTERNAL TABLE customer_sales (
    customer_id     BIGINT,
    customer_name   STRING,
    country         STRING,
    total_orders    BIGINT,
    total_quantity  BIGINT,
    total_spending  DOUBLE
)
STORED AS PARQUET
LOCATION '/analytics_zone/ecommerce/customer_sales';

-- ------------------------------------------------------------
-- product_sales
-- ------------------------------------------------------------
DROP TABLE IF EXISTS product_sales;

CREATE EXTERNAL TABLE product_sales (
    product_id      BIGINT,
    product_name    STRING,
    category        STRING,
    total_quantity  BIGINT,
    total_sales     DOUBLE
)
STORED AS PARQUET
LOCATION '/analytics_zone/ecommerce/product_sales';

-- ------------------------------------------------------------
-- monthly_sales / yearly_sales (extra rollups)
-- ------------------------------------------------------------
DROP TABLE IF EXISTS monthly_sales;

CREATE EXTERNAL TABLE monthly_sales (
    year            INT,
    month           INT,
    total_orders    BIGINT,
    total_quantity  BIGINT,
    total_sales     DOUBLE
)
STORED AS PARQUET
LOCATION '/analytics_zone/ecommerce/monthly_sales';

DROP TABLE IF EXISTS yearly_sales;

CREATE EXTERNAL TABLE yearly_sales (
    year            INT,
    total_orders    BIGINT,
    total_quantity  BIGINT,
    total_sales     DOUBLE
)
STORED AS PARQUET
LOCATION '/analytics_zone/ecommerce/yearly_sales';

-- ------------------------------------------------------------
-- sales_by_country / sales_by_category
-- ------------------------------------------------------------
DROP TABLE IF EXISTS sales_by_country;

CREATE EXTERNAL TABLE sales_by_country (
    country         STRING,
    total_sales     DOUBLE,
    total_quantity  BIGINT
)
STORED AS PARQUET
LOCATION '/analytics_zone/ecommerce/sales_by_country';

DROP TABLE IF EXISTS sales_by_category;

CREATE EXTERNAL TABLE sales_by_category (
    category        STRING,
    total_sales     DOUBLE,
    total_quantity  BIGINT
)
STORED AS PARQUET
LOCATION '/analytics_zone/ecommerce/sales_by_category';

-- ------------------------------------------------------------
-- quick sanity check
-- ------------------------------------------------------------
SHOW TABLES;
SELECT COUNT(*) AS fact_sales_rows FROM fact_sales;
SELECT COUNT(*) AS daily_sales_rows FROM daily_sales;
SELECT COUNT(*) AS customer_sales_rows FROM customer_sales;
SELECT COUNT(*) AS product_sales_rows FROM product_sales;
