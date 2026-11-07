-- Databricks notebook source
-- Run after the Gold dimension and fact notebooks.

CREATE SCHEMA IF NOT EXISTS adb_ecom_dev_01.gold;

CREATE TABLE IF NOT EXISTS adb_ecom_dev_01.gold.data_quality_results (
    run_timestamp TIMESTAMP,
    check_name STRING,
    table_name STRING,
    failed_count BIGINT,
    status STRING
) USING DELTA;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW dq_current_results AS
WITH checks AS (

    -- Silver business keys
    SELECT 'silver_customer_key_null' check_name, 'silver.customer' table_name,
           count_if(customer_id IS NULL OR trim(customer_id) = '') failed_count
    FROM adb_ecom_dev_01.silver.customer
    UNION ALL
    SELECT 'silver_customer_key_duplicate', 'silver.customer',
           count(*) FROM (
               SELECT customer_id FROM adb_ecom_dev_01.silver.customer
               GROUP BY customer_id HAVING count(*) > 1
           )
    UNION ALL
    SELECT 'silver_orders_key_null', 'silver.orders',
           count_if(order_id IS NULL OR trim(order_id) = '')
    FROM adb_ecom_dev_01.silver.orders
    UNION ALL
    SELECT 'silver_orders_key_duplicate', 'silver.orders',
           count(*) FROM (
               SELECT order_id FROM adb_ecom_dev_01.silver.orders
               GROUP BY order_id HAVING count(*) > 1
           )
    UNION ALL
    SELECT 'silver_order_items_key_null', 'silver.order_items',
           count_if(order_id IS NULL OR order_item_id IS NULL)
    FROM adb_ecom_dev_01.silver.order_items
    UNION ALL
    SELECT 'silver_order_items_key_duplicate', 'silver.order_items',
           count(*) FROM (
               SELECT order_id, order_item_id FROM adb_ecom_dev_01.silver.order_items
               GROUP BY order_id, order_item_id HAVING count(*) > 1
           )
    UNION ALL
    SELECT 'silver_order_payments_key_null', 'silver.order_payments',
           count_if(order_id IS NULL OR payment_sequential IS NULL)
    FROM adb_ecom_dev_01.silver.order_payments
    UNION ALL
    SELECT 'silver_order_payments_key_duplicate', 'silver.order_payments',
           count(*) FROM (
               SELECT order_id, payment_sequential FROM adb_ecom_dev_01.silver.order_payments
               GROUP BY order_id, payment_sequential HAVING count(*) > 1
           )
    UNION ALL
    SELECT 'silver_order_reviews_key_null', 'silver.order_reviews',
           count_if(review_id IS NULL OR order_id IS NULL)
    FROM adb_ecom_dev_01.silver.order_reviews
    UNION ALL
    SELECT 'silver_order_reviews_key_duplicate', 'silver.order_reviews',
           count(*) FROM (
               SELECT review_id, order_id FROM adb_ecom_dev_01.silver.order_reviews
               GROUP BY review_id, order_id HAVING count(*) > 1
           )
    UNION ALL
    SELECT 'silver_products_key_null', 'silver.products',
           count_if(product_id IS NULL OR trim(product_id) = '')
    FROM adb_ecom_dev_01.silver.products
    UNION ALL
    SELECT 'silver_products_key_duplicate', 'silver.products',
           count(*) FROM (
               SELECT product_id FROM adb_ecom_dev_01.silver.products
               GROUP BY product_id HAVING count(*) > 1
           )
    UNION ALL
    SELECT 'silver_sellers_key_null', 'silver.sellers',
           count_if(seller_id IS NULL OR trim(seller_id) = '')
    FROM adb_ecom_dev_01.silver.sellers
    UNION ALL
    SELECT 'silver_sellers_key_duplicate', 'silver.sellers',
           count(*) FROM (
               SELECT seller_id FROM adb_ecom_dev_01.silver.sellers
               GROUP BY seller_id HAVING count(*) > 1
           )

    -- Gold dimension keys
    UNION ALL
    SELECT 'gold_dim_customer_key_duplicate', 'gold.dim_customer',
           count(*) FROM (
               SELECT customer_id FROM adb_ecom_dev_01.gold.dim_customer
               GROUP BY customer_id HAVING count(*) > 1
           )
    UNION ALL
    SELECT 'gold_dim_product_key_duplicate', 'gold.dim_product',
           count(*) FROM (
               SELECT product_id FROM adb_ecom_dev_01.gold.dim_product
               GROUP BY product_id HAVING count(*) > 1
           )
    UNION ALL
    SELECT 'gold_dim_seller_key_duplicate', 'gold.dim_seller',
           count(*) FROM (
               SELECT seller_id FROM adb_ecom_dev_01.gold.dim_seller
               GROUP BY seller_id HAVING count(*) > 1
           )
    UNION ALL
    SELECT 'gold_dim_date_key_duplicate', 'gold.dim_date',
           count(*) FROM (
               SELECT date_key FROM adb_ecom_dev_01.gold.dim_date
               GROUP BY date_key HAVING count(*) > 1
           )

    -- Gold fact grain
    UNION ALL
    SELECT 'gold_fact_orders_duplicate_grain', 'gold.fact_orders',
           count(*) FROM (
               SELECT order_id FROM adb_ecom_dev_01.gold.fact_orders
               GROUP BY order_id HAVING count(*) > 1
           )
    UNION ALL
    SELECT 'gold_fact_order_items_duplicate_grain', 'gold.fact_order_items',
           count(*) FROM (
               SELECT order_id, order_item_id FROM adb_ecom_dev_01.gold.fact_order_items
               GROUP BY order_id, order_item_id HAVING count(*) > 1
           )
    UNION ALL
    SELECT 'gold_fact_order_payments_duplicate_grain', 'gold.fact_order_payments',
           count(*) FROM (
               SELECT order_id, payment_sequential FROM adb_ecom_dev_01.gold.fact_order_payments
               GROUP BY order_id, payment_sequential HAVING count(*) > 1
           )
    UNION ALL
    SELECT 'gold_fact_order_reviews_duplicate_grain', 'gold.fact_order_reviews',
           count(*) FROM (
               SELECT review_id, order_id FROM adb_ecom_dev_01.gold.fact_order_reviews
               GROUP BY review_id, order_id HAVING count(*) > 1
           )

    -- Dimension references in facts
    UNION ALL
    SELECT 'fact_orders_missing_customer', 'gold.fact_orders',
           count_if(f.customer_key IS NULL OR d.customer_key IS NULL)
    FROM adb_ecom_dev_01.gold.fact_orders f
    LEFT JOIN adb_ecom_dev_01.gold.dim_customer d ON f.customer_key = d.customer_key
    UNION ALL
    SELECT 'fact_items_missing_customer', 'gold.fact_order_items',
           count_if(f.customer_key IS NULL OR d.customer_key IS NULL)
    FROM adb_ecom_dev_01.gold.fact_order_items f
    LEFT JOIN adb_ecom_dev_01.gold.dim_customer d ON f.customer_key = d.customer_key
    UNION ALL
    SELECT 'fact_items_missing_product', 'gold.fact_order_items',
           count_if(f.product_key IS NULL OR d.product_key IS NULL)
    FROM adb_ecom_dev_01.gold.fact_order_items f
    LEFT JOIN adb_ecom_dev_01.gold.dim_product d ON f.product_key = d.product_key
    UNION ALL
    SELECT 'fact_items_missing_seller', 'gold.fact_order_items',
           count_if(f.seller_key IS NULL OR d.seller_key IS NULL)
    FROM adb_ecom_dev_01.gold.fact_order_items f
    LEFT JOIN adb_ecom_dev_01.gold.dim_seller d ON f.seller_key = d.seller_key
    UNION ALL
    SELECT 'fact_payments_missing_customer', 'gold.fact_order_payments',
           count_if(f.customer_key IS NULL OR d.customer_key IS NULL)
    FROM adb_ecom_dev_01.gold.fact_order_payments f
    LEFT JOIN adb_ecom_dev_01.gold.dim_customer d ON f.customer_key = d.customer_key
    UNION ALL
    SELECT 'fact_reviews_missing_customer', 'gold.fact_order_reviews',
           count_if(f.customer_key IS NULL OR d.customer_key IS NULL)
    FROM adb_ecom_dev_01.gold.fact_order_reviews f
    LEFT JOIN adb_ecom_dev_01.gold.dim_customer d ON f.customer_key = d.customer_key

    -- Basic value checks
    UNION ALL
    SELECT 'fact_payment_negative_value', 'gold.fact_order_payments',
           count_if(payment_value < 0)
    FROM adb_ecom_dev_01.gold.fact_order_payments
    UNION ALL
    SELECT 'fact_review_score_out_of_range', 'gold.fact_order_reviews',
           count_if(review_score IS NOT NULL AND (review_score < 1 OR review_score > 5))
    FROM adb_ecom_dev_01.gold.fact_order_reviews
)
SELECT current_timestamp() run_timestamp, check_name, table_name,
       coalesce(failed_count, 0) failed_count,
       CASE WHEN coalesce(failed_count, 0) = 0 THEN 'PASS' ELSE 'FAIL' END status
FROM checks;

-- COMMAND ----------

INSERT INTO adb_ecom_dev_01.gold.data_quality_results
SELECT * FROM dq_current_results;

-- Show this run's results.
SELECT check_name, table_name, failed_count, status
FROM dq_current_results
ORDER BY status DESC, table_name, check_name;

-- COMMAND ----------

-- Fail the notebook when any check fails. ADF can use this activity status
-- to stop downstream tasks and mark the pipeline run as failed.
SELECT CASE
    WHEN count_if(status = 'FAIL') > 0
    THEN raise_error(concat(
        'Data quality checks failed: ',
        cast(count_if(status = 'FAIL') as STRING),
        ' check(s). Review gold.data_quality_results for details.'
    ))
    ELSE 'All data quality checks passed.'
END AS data_quality_status
FROM dq_current_results;
