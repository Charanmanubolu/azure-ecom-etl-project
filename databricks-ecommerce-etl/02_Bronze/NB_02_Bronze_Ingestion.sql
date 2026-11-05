-- Databricks notebook source
-- MAGIC %python
-- MAGIC ../01_Configuration/01_Project_Config

-- COMMAND ----------

-- MAGIC %python
-- MAGIC source_path = raw_date_path("category_translation", 2026, 10, 31)
-- MAGIC df = spark.read.format("parquet").load(source_path)
-- MAGIC display(df)