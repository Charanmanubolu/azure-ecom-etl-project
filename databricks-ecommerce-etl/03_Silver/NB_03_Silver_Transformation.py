# Databricks notebook source
# MAGIC %run ../01_Configuration/01_Project_Config

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid4().hex[:8]
SILVER_TABLES = {e: f"{UC_CATALOG}.{SILVER_SCHEMA}.{e}" for e in RAW_ENTITIES}
QUARANTINE_TABLE = f"{UC_CATALOG}.{SILVER_SCHEMA}.data_quality_quarantine"
AUDIT_TABLE = f"{UC_CATALOG}.{SILVER_SCHEMA}.processing_audit"

HEADER_RENAMES = {
    "customer": {"Prop_0":"customer_id","Prop_1":"customer_unique_id","Prop_2":"customer_zip_code_prefix","Prop_3":"customer_city","Prop_4":"customer_state"},
    "order_payments": {"Prop_0":"order_id","Prop_1":"payment_sequential","Prop_2":"payment_type","Prop_3":"payment_installments","Prop_4":"payment_value"},
    "order_reviews": {"Prop_0":"review_id","Prop_1":"order_id","Prop_2":"review_score","Prop_3":"review_comment_title","Prop_4":"review_comment_message","Prop_5":"review_creation_date","Prop_6":"review_answer_timestamp"},
}
COLUMN_RENAMES = {
    "order_items": {"ORDER_ID":"order_id","ORDER_ITEM_ID":"order_item_id","PRODUCT_ID":"product_id","SELLER_ID":"seller_id","SHIPPING_LIMIT_DATE":"shipping_limit_date","PRICE":"price","FREIGHT_VALUE":"freight_value"},
    "products": {"product_name_lenght":"product_name_length","product_description_lenght":"product_description_length"},
}
BUSINESS_KEYS = {
    "category_translation":["product_category_name"],
    "customer":["customer_id"],
    "geolocation":["geolocation_zip_code_prefix","geolocation_lat","geolocation_lng","geolocation_city","geolocation_state"],
    "jsontest":["_id"],
    "order_items":["order_id","order_item_id"],
    "order_payments":["order_id","payment_sequential"],
    "order_reviews":["review_id"],
    "orders":["order_id"],
    "product_category_name_translation":["product_category_name"],
    "products":["product_id"],
    "sellers":["seller_id"],
}
CASTS = {
    "customer":{"customer_zip_code_prefix":"string"},
    "geolocation":{"geolocation_zip_code_prefix":"string","geolocation_lat":"decimal(10,7)","geolocation_lng":"decimal(10,7)"},
    "order_items":{"order_item_id":"int","shipping_limit_date":"timestamp","price":"decimal(12,2)","freight_value":"decimal(12,2)"},
    "order_payments":{"payment_sequential":"int","payment_installments":"int","payment_value":"decimal(12,2)"},
    "order_reviews":{"review_score":"int","review_creation_date":"timestamp","review_answer_timestamp":"timestamp"},
    "orders":{"order_purchase_timestamp":"timestamp","order_approved_at":"timestamp","order_delivered_carrier_date":"timestamp","order_delivered_customer_date":"timestamp","order_estimated_delivery_date":"timestamp"},
    "products":{"product_name_length":"int","product_description_length":"int","product_photos_qty":"int","product_weight_g":"decimal(12,2)","product_length_cm":"decimal(10,2)","product_height_cm":"decimal(10,2)","product_width_cm":"decimal(10,2)"},
    "sellers":{"seller_zip_code_prefix":"string"},
    "jsontest":{"index":"int","isActive":"boolean","age":"int","balance":"decimal(14,2)","latitude":"decimal(10,7)","longitude":"decimal(10,7)","registered":"timestamp"},
}

# COMMAND ----------

def ensure_support_tables():
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
        dq_record_id STRING, entity STRING, rejection_reason STRING,
        source_file STRING, ingested_at TIMESTAMP, run_id STRING,
        raw_record STRING, quarantined_at TIMESTAMP
      ) USING DELTA
    """)
    spark.sql(f"""
      CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
        run_id STRING, entity STRING, source_rows BIGINT, accepted_rows BIGINT,
        rejected_rows BIGINT, target_rows BIGINT, processed_at TIMESTAMP, status STRING
      ) USING DELTA
    """)

def standardize(entity, source):
    original_columns = source.columns
    df = source.withColumn("_raw_record", F.to_json(F.struct(*[F.col(c) for c in original_columns])))
    df = df.withColumn("_dq_reason", F.lit(None).cast("string"))

    # Remove header records that were landed as data in three source extracts.
    for first_col, expected in {
        "customer":("Prop_0","customer_id"),
        "order_payments":("Prop_0","order_id"),
        "order_reviews":("Prop_0","review_id"),
    }.items():
        if entity == first_col and expected[0] in df.columns:
            df = df.filter(F.lower(F.trim(F.col(expected[0]).cast("string"))) != expected[1])

    renames = {}
    renames.update(HEADER_RENAMES.get(entity, {}))
    renames.update(COLUMN_RENAMES.get(entity, {}))
    for old, new in renames.items():
        if old in df.columns and new not in df.columns:
            df = df.withColumnRenamed(old, new)

    for name, data_type in CASTS.get(entity, {}).items():
        if name in df.columns and data_type != "string":
            before = F.col(name)
            converted = F.expr(f"try_cast({name} as {data_type})")
            df = df.withColumn(name, converted)
            bad = before.isNotNull() & F.col(name).isNull()
            df = df.withColumn("_dq_reason", F.when(
                bad, F.concat_ws(";", F.col("_dq_reason"), F.lit("invalid_" + name))
            ).otherwise(F.col("_dq_reason")))
        elif name in df.columns:
            df = df.withColumn(name, F.col(name).cast("string"))
    return df

def quarantine(entity, rejected):
    if rejected.limit(1).count() == 0:
        return 0
    q = rejected.select(
        F.sha2(F.concat_ws("||", F.lit(entity), F.col("_source_file"),
                           F.col("_dq_reason"), F.col("_raw_record")), 256).alias("dq_record_id"),
        F.lit(entity).alias("entity"),
        F.col("_dq_reason").alias("rejection_reason"),
        F.col("_source_file").cast("string").alias("source_file"),
        F.col("_ingested_at").cast("timestamp").alias("ingested_at"),
        F.lit(RUN_ID).alias("run_id"),
        F.col("_raw_record").alias("raw_record"),
        F.current_timestamp().alias("quarantined_at"),
    )
    target = DeltaTable.forName(spark, QUARANTINE_TABLE)
    target.alias("t").merge(q.alias("s"), "t.dq_record_id = s.dq_record_id").whenNotMatchedInsertAll().execute()
    return q.count()

def merge_silver(entity, accepted, keys):
    table_name = SILVER_TABLES[entity]
    if not spark.catalog.tableExists(table_name):
        accepted.write.format("delta").mode("overwrite").saveAsTable(table_name)
        return accepted.count()
    condition = " AND ".join([f"t.{k} = s.{k}" for k in keys])
    values = {c: f"s.{c}" for c in accepted.columns}
    (DeltaTable.forName(spark, table_name).alias("t")
      .merge(accepted.alias("s"), condition)
      .whenMatchedUpdate(set=values)
      .whenNotMatchedInsert(values=values)
      .execute())
    return accepted.count()

def process_entity(entity):
    source_name = f"{UC_CATALOG}.{BRONZE_SCHEMA}.{entity}"
    if not spark.catalog.tableExists(source_name):
        raise ValueError(f"Required Bronze table is missing: {source_name}")
    source = spark.table(source_name)
    source_rows = source.count()
    if source_rows == 0:
        raise ValueError(f"Bronze table is empty: {source_name}")

    df = standardize(entity, source)
    keys = BUSINESS_KEYS[entity]
    missing = [k for k in keys if k not in df.columns]
    if missing:
        raise ValueError(f"{entity}: configured key columns missing: {missing}; actual columns: {df.columns}")

    invalid_key = None
    for key in keys:
        bad = F.col(key).isNull() | (F.trim(F.col(key).cast("string")) == "")
        invalid_key = bad if invalid_key is None else invalid_key | bad
    df = df.withColumn("_dq_reason", F.when(
        invalid_key, F.concat_ws(";", F.col("_dq_reason"), F.lit("missing_business_key"))
    ).otherwise(F.col("_dq_reason")))

    rejected = df.filter(F.col("_dq_reason").isNotNull()).select("_dq_reason","_source_file","_ingested_at","_raw_record")
    rejected_rows = quarantine(entity, rejected)
    accepted = df.filter(F.col("_dq_reason").isNull()).drop("_dq_reason","_raw_record")

    # Source folders contain repeated full snapshots. Keep the latest Type 1 row per key.
    ordering = []
    for col_name in ["_ingested_at","_source_file_modification_time","_source_file"]:
        if col_name in accepted.columns:
            ordering.append(F.col(col_name).desc_nulls_last())
    if not ordering:
        ordering = [F.col(keys[0]).asc()]
    w = Window.partitionBy(*[F.col(k) for k in keys]).orderBy(*ordering)
    accepted = accepted.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

    if accepted.limit(1).count() == 0:
        raise ValueError(f"{entity}: no accepted rows remain after validation.")
    duplicate = accepted.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count()
    if duplicate:
        raise ValueError(f"{entity}: duplicate source keys remain after deduplication.")

    accepted_rows = accepted.count()
    target_rows = merge_silver(entity, accepted, keys)
    spark.createDataFrame([(
        RUN_ID, entity, int(source_rows), int(accepted_rows), int(rejected_rows),
        int(target_rows), datetime.now(timezone.utc), "SUCCESS"
    )], """
      run_id STRING, entity STRING, source_rows LONG, accepted_rows LONG,
      rejected_rows LONG, target_rows LONG, processed_at TIMESTAMP, status STRING
    """).write.format("delta").mode("append").saveAsTable(AUDIT_TABLE)

    return (entity, source_rows, accepted_rows, rejected_rows, target_rows, "SUCCESS")

# COMMAND ----------

ensure_support_tables()

# Process each configured Bronze entity. A structural error stops the run;
# previously completed entities can be replayed safely because Silver uses MERGE.
results = []
for entity in RAW_ENTITIES:
    print(f"Processing {entity}")
    results.append(process_entity(entity))

display(spark.createDataFrame(
    results,
    ["entity","source_rows","accepted_rows","rejected_rows","target_rows_reported","status"]
))

# COMMAND ----------

inventory = [
    (entity, SILVER_TABLES[entity], spark.table(SILVER_TABLES[entity]).count())
    for entity in RAW_ENTITIES
]
display(spark.createDataFrame(inventory, ["entity","silver_table","row_count"]))
print("Silver run ID:", RUN_ID)
