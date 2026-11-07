-- Databricks notebook source
-- Gold dimensions. Current-state attributes only; SCD history is deferred.
CREATE SCHEMA IF NOT EXISTS adb_ecom_dev_01.gold;

CREATE TABLE IF NOT EXISTS adb_ecom_dev_01.gold.dim_customer (
 customer_key STRING, customer_id STRING, customer_unique_id STRING,
 customer_zip_code_prefix STRING, customer_city STRING, customer_state STRING,
 created_at TIMESTAMP, updated_at TIMESTAMP
) USING DELTA;
CREATE TABLE IF NOT EXISTS adb_ecom_dev_01.gold.dim_product (
 product_key STRING, product_id STRING, product_category_name STRING,
 product_category_name_english STRING, product_name_length INT,
 product_description_length INT, product_photos_qty INT,
 product_weight_g DECIMAL(18,2), product_length_cm DECIMAL(18,2),
 product_height_cm DECIMAL(18,2), product_width_cm DECIMAL(18,2),
 created_at TIMESTAMP, updated_at TIMESTAMP
) USING DELTA;
CREATE TABLE IF NOT EXISTS adb_ecom_dev_01.gold.dim_seller (
 seller_key STRING, seller_id STRING, seller_zip_code_prefix STRING,
 seller_city STRING, seller_state STRING, created_at TIMESTAMP, updated_at TIMESTAMP
) USING DELTA;
CREATE TABLE IF NOT EXISTS adb_ecom_dev_01.gold.dim_date (
 date_key INT, full_date DATE, calendar_year INT, calendar_quarter INT,
 calendar_month INT, month_name STRING, day_of_month INT, day_of_week INT,
 day_name STRING, is_weekend BOOLEAN
) USING DELTA;

-- COMMAND ----------

MERGE INTO adb_ecom_dev_01.gold.dim_customer t
USING (
 SELECT sha2(customer_id,256) customer_key, customer_id, customer_unique_id,
        customer_zip_code_prefix, customer_city, customer_state
 FROM (
  SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY customer_unique_id) rn
  FROM adb_ecom_dev_01.silver.customer WHERE customer_id IS NOT NULL
 ) WHERE rn=1
) s ON t.customer_id=s.customer_id
WHEN MATCHED THEN UPDATE SET customer_key=s.customer_key,
 customer_unique_id=s.customer_unique_id, customer_zip_code_prefix=s.customer_zip_code_prefix,
 customer_city=s.customer_city, customer_state=s.customer_state, updated_at=current_timestamp()
WHEN NOT MATCHED THEN INSERT VALUES
 (s.customer_key,s.customer_id,s.customer_unique_id,s.customer_zip_code_prefix,
  s.customer_city,s.customer_state,current_timestamp(),current_timestamp());

-- COMMAND ----------

MERGE INTO adb_ecom_dev_01.gold.dim_product t
USING (
 WITH tr AS (
  SELECT product_category_name, product_category_name_english FROM (
   SELECT *, row_number() OVER (PARTITION BY product_category_name ORDER BY product_category_name_english) rn
   FROM adb_ecom_dev_01.silver.product_category_name_translation
  ) WHERE rn=1
 )
 SELECT sha2(p.product_id,256) product_key,p.product_id,p.product_category_name,
        tr.product_category_name_english,p.product_name_length,p.product_description_length,
        p.product_photos_qty,p.product_weight_g,p.product_length_cm,p.product_height_cm,p.product_width_cm
 FROM (
  SELECT *,row_number() OVER (PARTITION BY product_id ORDER BY product_category_name) rn
  FROM adb_ecom_dev_01.silver.products WHERE product_id IS NOT NULL
 ) p LEFT JOIN tr ON p.product_category_name=tr.product_category_name WHERE p.rn=1
) s ON t.product_id=s.product_id
WHEN MATCHED THEN UPDATE SET product_key=s.product_key,
 product_category_name=s.product_category_name,
 product_category_name_english=s.product_category_name_english,
 product_name_length=s.product_name_length, product_description_length=s.product_description_length,
 product_photos_qty=s.product_photos_qty, product_weight_g=s.product_weight_g,
 product_length_cm=s.product_length_cm, product_height_cm=s.product_height_cm,
 product_width_cm=s.product_width_cm, updated_at=current_timestamp()
WHEN NOT MATCHED THEN INSERT VALUES
 (s.product_key,s.product_id,s.product_category_name,s.product_category_name_english,
  s.product_name_length,s.product_description_length,s.product_photos_qty,s.product_weight_g,
  s.product_length_cm,s.product_height_cm,s.product_width_cm,current_timestamp(),current_timestamp());

-- COMMAND ----------

MERGE INTO adb_ecom_dev_01.gold.dim_seller t
USING (
 SELECT sha2(seller_id,256) seller_key,seller_id,seller_zip_code_prefix,seller_city,seller_state
 FROM (
  SELECT *,row_number() OVER (PARTITION BY seller_id ORDER BY seller_city,seller_state) rn
  FROM adb_ecom_dev_01.silver.sellers WHERE seller_id IS NOT NULL
 ) WHERE rn=1
) s ON t.seller_id=s.seller_id
WHEN MATCHED THEN UPDATE SET seller_key=s.seller_key,
 seller_zip_code_prefix=s.seller_zip_code_prefix,seller_city=s.seller_city,
 seller_state=s.seller_state,updated_at=current_timestamp()
WHEN NOT MATCHED THEN INSERT VALUES
 (s.seller_key,s.seller_id,s.seller_zip_code_prefix,s.seller_city,s.seller_state,
  current_timestamp(),current_timestamp());

-- COMMAND ----------

MERGE INTO adb_ecom_dev_01.gold.dim_date t
USING (
 WITH ds AS (
  SELECT to_date(order_purchase_timestamp) d FROM adb_ecom_dev_01.silver.orders
  UNION ALL SELECT to_date(order_approved_at) FROM adb_ecom_dev_01.silver.orders
  UNION ALL SELECT to_date(order_delivered_carrier_date) FROM adb_ecom_dev_01.silver.orders
  UNION ALL SELECT to_date(order_delivered_customer_date) FROM adb_ecom_dev_01.silver.orders
  UNION ALL SELECT to_date(order_estimated_delivery_date) FROM adb_ecom_dev_01.silver.orders
  UNION ALL SELECT to_date(shipping_limit_date) FROM adb_ecom_dev_01.silver.order_items
  UNION ALL SELECT to_date(review_creation_date) FROM adb_ecom_dev_01.silver.order_reviews
  UNION ALL SELECT to_date(review_answer_timestamp) FROM adb_ecom_dev_01.silver.order_reviews
 ), bounds AS (SELECT min(d) lo,max(d) hi FROM ds WHERE d IS NOT NULL),
 cal AS (SELECT explode(sequence(lo,hi,interval 1 day)) d FROM bounds WHERE lo IS NOT NULL)
 SELECT cast(date_format(d,'yyyyMMdd') as INT) date_key,d full_date,year(d) calendar_year,
        quarter(d) calendar_quarter,month(d) calendar_month,date_format(d,'MMMM') month_name,
        dayofmonth(d) day_of_month,dayofweek(d) day_of_week,date_format(d,'EEEE') day_name,
        dayofweek(d) IN (1,7) is_weekend FROM cal
) s ON t.date_key=s.date_key
WHEN MATCHED THEN UPDATE SET full_date=s.full_date,calendar_year=s.calendar_year,
 calendar_quarter=s.calendar_quarter,calendar_month=s.calendar_month,month_name=s.month_name,
 day_of_month=s.day_of_month,day_of_week=s.day_of_week,day_name=s.day_name,is_weekend=s.is_weekend
WHEN NOT MATCHED THEN INSERT *;

MERGE INTO adb_ecom_dev_01.gold.dim_date t
USING (SELECT -1 date_key,cast(NULL as DATE) full_date,cast(NULL as INT) calendar_year,
 cast(NULL as INT) calendar_quarter,cast(NULL as INT) calendar_month,'Unknown' month_name,
 cast(NULL as INT) day_of_month,cast(NULL as INT) day_of_week,'Unknown' day_name,false is_weekend) s
ON t.date_key=s.date_key WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

SELECT 'dim_customer' table_name,count(*) row_count,count(DISTINCT customer_id) distinct_keys
FROM adb_ecom_dev_01.gold.dim_customer
UNION ALL SELECT 'dim_product',count(*),count(DISTINCT product_id) FROM adb_ecom_dev_01.gold.dim_product
UNION ALL SELECT 'dim_seller',count(*),count(DISTINCT seller_id) FROM adb_ecom_dev_01.gold.dim_seller
UNION ALL SELECT 'dim_date',count(*),count(DISTINCT date_key) FROM adb_ecom_dev_01.gold.dim_date;
