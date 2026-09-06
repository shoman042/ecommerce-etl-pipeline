-- ============================================================
-- Data Quality & Record-Count Validation - Run this in Hive ONLY
-- Usage: hive -f validation_hive.sql
-- ============================================================

USE ecommerce_dw;

-- record counts
SELECT 'fact_sales' AS tbl, COUNT(*) AS cnt FROM fact_sales
UNION ALL
SELECT 'daily_sales', COUNT(*) FROM daily_sales
UNION ALL
SELECT 'customer_sales', COUNT(*) FROM customer_sales
UNION ALL
SELECT 'product_sales', COUNT(*) FROM product_sales;

-- fact_sales should have no nulls in key columns
SELECT COUNT(*) AS null_order_id      FROM fact_sales WHERE order_id IS NULL;
SELECT COUNT(*) AS null_customer_id   FROM fact_sales WHERE customer_id IS NULL;
SELECT COUNT(*) AS null_product_id    FROM fact_sales WHERE product_id IS NULL;
SELECT COUNT(*) AS null_order_date    FROM fact_sales WHERE order_date IS NULL;

-- no negative or zero prices/quantities should exist post-cleaning
SELECT COUNT(*) AS bad_quantity   FROM fact_sales WHERE quantity <= 0;
SELECT COUNT(*) AS bad_unit_price FROM fact_sales WHERE unit_price <= 0;
SELECT COUNT(*) AS bad_total      FROM fact_sales WHERE total_amount <= 0;

-- total_amount must equal quantity * unit_price (rounding tolerance)
SELECT COUNT(*) AS mismatched_totals
FROM fact_sales
WHERE ABS(total_amount - (quantity * unit_price)) > 0.01;

-- dates should not be in the future
SELECT COUNT(*) AS future_dates FROM fact_sales WHERE order_date > CURRENT_TIMESTAMP();

-- sum of daily_sales.total_sales should reconcile with fact_sales total (approx)
SELECT
    (SELECT ROUND(SUM(total_amount), 2) FROM fact_sales)  AS fact_sales_total,
    (SELECT ROUND(SUM(total_sales), 2)  FROM daily_sales) AS daily_sales_total;

-- customer_sales total_spending should reconcile with fact_sales
SELECT
    (SELECT ROUND(SUM(total_amount), 2) FROM fact_sales)      AS fact_sales_total,
    (SELECT ROUND(SUM(total_spending), 2) FROM customer_sales) AS customer_sales_total;

-- product_sales total_sales should reconcile with fact_sales
SELECT
    (SELECT ROUND(SUM(total_amount), 2) FROM fact_sales)     AS fact_sales_total,
    (SELECT ROUND(SUM(total_sales), 2) FROM product_sales)   AS product_sales_total;

-- duplicate check on fact_sales natural key (order_id + product_id)
SELECT order_id, product_id, COUNT(*) AS dup_count
FROM fact_sales
GROUP BY order_id, product_id
HAVING COUNT(*) > 1
LIMIT 20;
