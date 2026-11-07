-- Databricks notebook source
-- Gold facts at explicit business grains. Rerunnable Delta MERGEs prevent duplicate facts.
-- Run NB_06_Dimension_Tables.sql before this notebook.

CREATE SCHEMA IF NOT EXISTS adb_ecom_dev_01.gold;

CREATE TABLE IF NOT EXISTS adb_ecom_dev_01.gold.fact_orders (
 order_id STRING, customer_key STRING, purchase_date_key INT, approved_date_key INT,
 delivered_carrier_date_key INT, delivered_customer_date_key INT, estimated_delivery_date_key INT,
 order_status STRING, order_count INT, created_at TIMESTAMP, updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS adb_ecom_dev_01.gold.fact_order_items (
 order_item_fact_key STRING, order_id STRING, order_item_id INT,
 customer_key STRING, product_key STRING, seller_key STRING,
 purchase_date_key INT, shipping_limit_date_key INT, order_status STRING,
 price DECIMAL(18,2), freight_value DECIMAL(18,2), item_total DECIMAL(18,2),
 item_count INT, created_at TIMESTAMP, updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS adb_ecom_dev_01.gold.fact_order_payments (
 payment_fact_key STRING, order_id STRING, payment_sequential INT,
 customer_key STRING, purchase_date_key INT, payment_type STRING,
 payment_installments INT, payment_value DECIMAL(18,2),
 payment_count INT, created_at TIMESTAMP, updated_at TIMESTAMP
) USING DELTA;

CREATE TABLE IF NOT EXISTS adb_ecom_dev_01.gold.fact_order_reviews (
 review_fact_key STRING, review_id STRING, order_id STRING, customer_key STRING,
 review_creation_date_key INT, review_answer_date_key INT, review_score INT,
 review_count INT, created_at TIMESTAMP, updated_at TIMESTAMP
) USING DELTA;

-- COMMAND ----------

-- Grain: one row per order_id.
MERGE INTO adb_ecom_dev_01.gold.fact_orders t
USING (
 SELECT o.order_id,c.customer_key,
        coalesce(cast(date_format(to_date(o.order_purchase_timestamp),'yyyyMMdd') as INT),-1) purchase_date_key,
        coalesce(cast(date_format(to_date(o.order_approved_at),'yyyyMMdd') as INT),-1) approved_date_key,
        coalesce(cast(date_format(to_date(o.order_delivered_carrier_date),'yyyyMMdd') as INT),-1) delivered_carrier_date_key,
        coalesce(cast(date_format(to_date(o.order_delivered_customer_date),'yyyyMMdd') as INT),-1) delivered_customer_date_key,
        coalesce(cast(date_format(to_date(o.order_estimated_delivery_date),'yyyyMMdd') as INT),-1) estimated_delivery_date_key,
        o.order_status,1 order_count
 FROM adb_ecom_dev_01.silver.orders o
 LEFT JOIN adb_ecom_dev_01.gold.dim_customer c ON o.customer_id=c.customer_id
 WHERE o.order_id IS NOT NULL
) s ON t.order_id=s.order_id
WHEN MATCHED THEN UPDATE SET customer_key=s.customer_key,
 purchase_date_key=s.purchase_date_key,approved_date_key=s.approved_date_key,
 delivered_carrier_date_key=s.delivered_carrier_date_key,
 delivered_customer_date_key=s.delivered_customer_date_key,
 estimated_delivery_date_key=s.estimated_delivery_date_key,
 order_status=s.order_status,order_count=s.order_count,updated_at=current_timestamp()
WHEN NOT MATCHED THEN INSERT VALUES
 (s.order_id,s.customer_key,s.purchase_date_key,s.approved_date_key,
  s.delivered_carrier_date_key,s.delivered_customer_date_key,s.estimated_delivery_date_key,
  s.order_status,s.order_count,current_timestamp(),current_timestamp());

-- COMMAND ----------

-- Grain: one row per (order_id, order_item_id).
MERGE INTO adb_ecom_dev_01.gold.fact_order_items t
USING (
 SELECT sha2(concat_ws('||',i.order_id,cast(i.order_item_id as STRING)),256) order_item_fact_key,
        i.order_id,i.order_item_id,c.customer_key,p.product_key,v.seller_key,
        coalesce(cast(date_format(to_date(o.order_purchase_timestamp),'yyyyMMdd') as INT),-1) purchase_date_key,
        coalesce(cast(date_format(to_date(i.shipping_limit_date),'yyyyMMdd') as INT),-1) shipping_limit_date_key,
        o.order_status,i.price,i.freight_value,
        cast(coalesce(i.price,0)+coalesce(i.freight_value,0) as DECIMAL(18,2)) item_total,
        1 item_count
 FROM adb_ecom_dev_01.silver.order_items i
 LEFT JOIN adb_ecom_dev_01.silver.orders o ON i.order_id=o.order_id
 LEFT JOIN adb_ecom_dev_01.gold.dim_customer c ON o.customer_id=c.customer_id
 LEFT JOIN adb_ecom_dev_01.gold.dim_product p ON i.product_id=p.product_id
 LEFT JOIN adb_ecom_dev_01.gold.dim_seller v ON i.seller_id=v.seller_id
 WHERE i.order_id IS NOT NULL AND i.order_item_id IS NOT NULL
) s ON t.order_id=s.order_id AND t.order_item_id=s.order_item_id
WHEN MATCHED THEN UPDATE SET order_item_fact_key=s.order_item_fact_key,
 customer_key=s.customer_key,product_key=s.product_key,seller_key=s.seller_key,
 purchase_date_key=s.purchase_date_key,shipping_limit_date_key=s.shipping_limit_date_key,
 order_status=s.order_status,price=s.price,freight_value=s.freight_value,
 item_total=s.item_total,item_count=s.item_count,updated_at=current_timestamp()
WHEN NOT MATCHED THEN INSERT VALUES
 (s.order_item_fact_key,s.order_id,s.order_item_id,s.customer_key,s.product_key,s.seller_key,
  s.purchase_date_key,s.shipping_limit_date_key,s.order_status,s.price,s.freight_value,
  s.item_total,s.item_count,current_timestamp(),current_timestamp());

-- COMMAND ----------

-- Grain: one row per (order_id, payment_sequential).
MERGE INTO adb_ecom_dev_01.gold.fact_order_payments t
USING (
 SELECT sha2(concat_ws('||',p.order_id,cast(p.payment_sequential as STRING)),256) payment_fact_key,
        p.order_id,p.payment_sequential,c.customer_key,
        coalesce(cast(date_format(to_date(o.order_purchase_timestamp),'yyyyMMdd') as INT),-1) purchase_date_key,
        p.payment_type,p.payment_installments,p.payment_value,1 payment_count
 FROM adb_ecom_dev_01.silver.order_payments p
 LEFT JOIN adb_ecom_dev_01.silver.orders o ON p.order_id=o.order_id
 LEFT JOIN adb_ecom_dev_01.gold.dim_customer c ON o.customer_id=c.customer_id
 WHERE p.order_id IS NOT NULL AND p.payment_sequential IS NOT NULL
) s ON t.order_id=s.order_id AND t.payment_sequential=s.payment_sequential
WHEN MATCHED THEN UPDATE SET payment_fact_key=s.payment_fact_key,
 customer_key=s.customer_key,purchase_date_key=s.purchase_date_key,
 payment_type=s.payment_type,payment_installments=s.payment_installments,
 payment_value=s.payment_value,payment_count=s.payment_count,updated_at=current_timestamp()
WHEN NOT MATCHED THEN INSERT VALUES
 (s.payment_fact_key,s.order_id,s.payment_sequential,s.customer_key,s.purchase_date_key,
  s.payment_type,s.payment_installments,s.payment_value,s.payment_count,
  current_timestamp(),current_timestamp());

-- COMMAND ----------

-- Grain: one row per (review_id, order_id).
MERGE INTO adb_ecom_dev_01.gold.fact_order_reviews t
USING (
 SELECT sha2(concat_ws('||',r.review_id,r.order_id),256) review_fact_key,
        r.review_id,r.order_id,c.customer_key,
        coalesce(cast(date_format(to_date(r.review_creation_date),'yyyyMMdd') as INT),-1) review_creation_date_key,
        coalesce(cast(date_format(to_date(r.review_answer_timestamp),'yyyyMMdd') as INT),-1) review_answer_date_key,
        r.review_score,1 review_count
 FROM adb_ecom_dev_01.silver.order_reviews r
 LEFT JOIN adb_ecom_dev_01.silver.orders o ON r.order_id=o.order_id
 LEFT JOIN adb_ecom_dev_01.gold.dim_customer c ON o.customer_id=c.customer_id
 WHERE r.review_id IS NOT NULL AND r.order_id IS NOT NULL
) s ON t.review_id=s.review_id AND t.order_id=s.order_id
WHEN MATCHED THEN UPDATE SET review_fact_key=s.review_fact_key,
 customer_key=s.customer_key,review_creation_date_key=s.review_creation_date_key,
 review_answer_date_key=s.review_answer_date_key,review_score=s.review_score,
 review_count=s.review_count,updated_at=current_timestamp()
WHEN NOT MATCHED THEN INSERT VALUES
 (s.review_fact_key,s.review_id,s.order_id,s.customer_key,
  s.review_creation_date_key,s.review_answer_date_key,s.review_score,s.review_count,
  current_timestamp(),current_timestamp());

-- COMMAND ----------

-- Reconciliation and grain checks.
SELECT 'fact_orders' table_name,count(*) row_count,count(DISTINCT order_id) distinct_grain
FROM adb_ecom_dev_01.gold.fact_orders
UNION ALL
SELECT 'fact_order_items',count(*),count(DISTINCT concat_ws('||',order_id,cast(order_item_id as STRING)))
FROM adb_ecom_dev_01.gold.fact_order_items
UNION ALL
SELECT 'fact_order_payments',count(*),count(DISTINCT concat_ws('||',order_id,cast(payment_sequential as STRING)))
FROM adb_ecom_dev_01.gold.fact_order_payments
UNION ALL
SELECT 'fact_order_reviews',count(*),count(DISTINCT concat_ws('||',review_id,order_id))
FROM adb_ecom_dev_01.gold.fact_order_reviews;

-- COMMAND ----------

-- Basic orphan-key checks for the item fact.
SELECT
 sum(CASE WHEN customer_key IS NULL THEN 1 ELSE 0 END) AS missing_customer_dimension,
 sum(CASE WHEN product_key IS NULL THEN 1 ELSE 0 END) AS missing_product_dimension,
 sum(CASE WHEN seller_key IS NULL THEN 1 ELSE 0 END) AS missing_seller_dimension
FROM adb_ecom_dev_01.gold.fact_order_items;
