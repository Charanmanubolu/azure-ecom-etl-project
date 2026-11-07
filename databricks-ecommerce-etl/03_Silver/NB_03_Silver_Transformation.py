# Databricks notebook source
# MAGIC %run ../01_Configuration/01_Project_Config

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid4().hex[:8]
AUDIT_COLUMNS = {"_ingested_at", "_source_file", "_source_file_modification_time", "_batch_id"}
QUARANTINE_TABLE = f"{SILVER_DB}.data_quality_quarantine"
AUDIT_TABLE = f"{SILVER_DB}.processing_audit"

# Keys are business-grain definitions, not inferred from column names.
ENTITY_KEYS = {
    "category_translation": ["product_category_name"],
    "customer": ["customer_id"],
    "geolocation": ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng", "geolocation_city", "geolocation_state"],
    "jsontest": ["_id"],
    "order_items": ["order_id", "order_item_id"],
    "order_payments": ["order_id", "payment_sequential"],
    "order_reviews": ["review_id"],
    "orders": ["order_id"],
    "product_category_name_translation": ["product_category_name"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
}

# Explicit, conservative casts. Postal codes and identifiers remain strings.
TYPE_CASTS = {
    "customer": {"customer_zip_code_prefix": "string"},
    "order_items": {"order_item_id": "int", "price": "decimal(18,2)", "freight_value": "decimal(18,2)"},
    "order_payments": {"payment_sequential": "int", "payment_installments": "int", "payment_value": "decimal(18,2)"},
    "order_reviews": {"review_score": "int"},
    "products": {"product_name_length": "int", "product_description_length": "int", "product_photos_qty": "int", "product_weight_g": "decimal(18,2)", "product_length_cm": "decimal(18,2)", "product_height_cm": "decimal(18,2)", "product_width_cm": "decimal(18,2)"},
    "sellers": {"seller_zip_code_prefix": "string"},
    "geolocation": {"geolocation_zip_code_prefix": "string", "geolocation_lat": "decimal(10,7)", "geolocation_lng": "decimal(10,7)"},
}

def ensure_support_tables():
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
        quarantine_id STRING, entity STRING, reason STRING, record_json STRING,
        source_file STRING, run_id STRING, quarantined_at TIMESTAMP
    ) USING DELTA""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {AUDIT_TABLE} (
        run_id STRING, entity STRING, status STRING, source_rows BIGINT,
        accepted_rows BIGINT, rejected_rows BIGINT, inserted_rows BIGINT,
        updated_rows BIGINT, started_at TIMESTAMP, completed_at TIMESTAMP, error_message STRING
    ) USING DELTA""")

def append_audit(entity, status, counts, started_at, error_message=None):
    row = [(RUN_ID, entity, status, int(counts.get("source_rows", 0)),
            int(counts.get("accepted_rows", 0)), int(counts.get("rejected_rows", 0)),
            int(counts.get("inserted_rows", 0)), int(counts.get("updated_rows", 0)),
            started_at, datetime.now(timezone.utc).replace(tzinfo=None), error_message)]
    cols = ["run_id", "entity", "status", "source_rows", "accepted_rows", "rejected_rows",
            "inserted_rows", "updated_rows", "started_at", "completed_at", "error_message"]
    spark.createDataFrame(row, cols).write.format("delta").mode("append").saveAsTable(AUDIT_TABLE)

def write_quarantine(entity, rejected, reason_col="_dq_reason"):
    if rejected.isEmpty():
        return 0
    payload_cols = [c for c in rejected.columns if c != reason_col]
    serialized = F.to_json(F.struct(*[F.col(c) for c in payload_cols]))
    q = (rejected.withColumn("_record_json", serialized)
         .withColumn("_quarantine_id", F.sha2(F.concat_ws("||", F.lit(entity), F.col(reason_col), F.col("_record_json")), 256))
         .select(F.col("_quarantine_id").alias("quarantine_id"),
                 F.lit(entity).alias("entity"), F.col(reason_col).alias("reason"),
                 F.col("_record_json").alias("record_json"),
                 F.col("_source_file").cast("string").alias("source_file"),
                 F.lit(RUN_ID).alias("run_id"), F.current_timestamp().alias("quarantined_at"))
         .dropDuplicates(["quarantine_id"]))
    target = DeltaTable.forName(spark, QUARANTINE_TABLE)
    (target.alias("t").merge(q.alias("s"), "t.quarantine_id = s.quarantine_id")
     .whenNotMatchedInsertAll().execute())
    return q.count()

def normalize_columns(entity, source):
    df = source
    if entity == "order_items":
        for old, new in [("ORDER_ID", "order_id"), ("ORDER_ITEM_ID", "order_item_id"),
                         ("PRODUCT_ID", "product_id"), ("SELLER_ID", "seller_id"),
                         ("SHIPPING_LIMIT_DATE", "shipping_limit_date"), ("PRICE", "price"),
                         ("FREIGHT_VALUE", "freight_value")]:
            if old in df.columns:
                df = df.withColumnRenamed(old, new)
    if entity == "products":
        for old, new in [("product_name_lenght", "product_name_length"),
                         ("product_description_lenght", "product_description_length")]:
            if old in df.columns:
                df = df.withColumnRenamed(old, new)
    # Older CSV-derived snapshots contain the original header as a data row.
    if entity == "customer" and "customer_id" in df.columns:
        df = df.filter(F.col("customer_id") != F.lit("customer_id"))
    if entity == "order_payments" and "order_id" in df.columns:
        df = df.filter(F.col("order_id") != F.lit("order_id"))
    if entity == "order_reviews" and "review_id" in df.columns:
        df = df.filter(F.col("review_id") != F.lit("review_id"))
    return df

def transform_types(entity, df):
    bad_conditions = []
    for col_name, target_type in TYPE_CASTS.get(entity, {}).items():
        if col_name not in df.columns:
            raise ValueError(f"{entity}: expected column '{col_name}' for configured type cast is missing")
        if target_type != "string":
            bad_conditions.append((col_name, target_type, F.col(col_name).isNotNull() & F.expr(f"try_cast(`{col_name}` as {target_type}) is null")))
    if bad_conditions:
        reason = F.concat_ws("; ", *[F.when(cond, F.lit(f"invalid {name} ({dtype})")) for name, dtype, cond in bad_conditions])
        df = df.withColumn("_dq_reason", reason)
        rejected = df.filter(F.col("_dq_reason") != "")
        accepted = df.filter((F.col("_dq_reason").isNull()) | (F.col("_dq_reason") == "")).drop("_dq_reason")
    else:
        rejected = df.limit(0).withColumn("_dq_reason", F.lit(None).cast("string"))
        accepted = df
    for col_name, target_type in TYPE_CASTS.get(entity, {}).items():
        accepted = accepted.withColumn(col_name, F.col(col_name).cast(target_type))
    return accepted, rejected

def process_entity(entity):
    started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    counts = {"source_rows": 0, "accepted_rows": 0, "rejected_rows": 0, "inserted_rows": 0, "updated_rows": 0}
    try:
        if entity not in ENTITY_KEYS:
            raise ValueError(f"No business-key definition is configured for '{entity}'")
        source_table = f"{BRONZE_DB}.{entity}"
        if not spark.catalog.tableExists(source_table):
            raise ValueError(f"Required Bronze table does not exist: {source_table}")
        source = spark.table(source_table)
        counts["source_rows"] = source.count()
        if counts["source_rows"] == 0:
            raise ValueError(f"Bronze source is empty: {source_table}")
        df = normalize_columns(entity, source)
        required = ENTITY_KEYS[entity]
        missing = [c for c in required if c not in df.columns]
        if missing:
            raise ValueError(f"{entity}: missing business key columns: {missing}")
        df, rejected_casts = transform_types(entity, df)
        write_quarantine(entity, rejected_casts)
        counts["rejected_rows"] += rejected_casts.count()
        key_null = None
        for key in required:
            condition = F.col(key).isNull() | (F.trim(F.col(key).cast("string")) == "")
            key_null = condition if key_null is None else (key_null | condition)
        rejected_keys = df.filter(key_null).withColumn("_dq_reason", F.lit("null_or_blank_business_key"))
        counts["rejected_rows"] += rejected_keys.count()
        write_quarantine(entity, rejected_keys)
        valid = df.filter(~key_null)
        business_cols = sorted([c for c in valid.columns if c not in AUDIT_COLUMNS and not c.startswith("_dq_")])
        if not business_cols:
            raise ValueError(f"{entity}: no business columns remain after normalization")
        valid = valid.withColumn("_business_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in business_cols])), 256))
        # A business key with multiple distinct payloads is unsafe; fail before MERGE.
        conflicts = (valid.groupBy(*required).agg(F.countDistinct("_business_hash").alias("payload_versions"))
                     .filter(F.col("payload_versions") > 1))
        conflict_count = conflicts.count()
        if conflict_count:
            sample = conflicts.limit(10).toJSON().collect()
            raise ValueError(f"{entity}: {conflict_count} conflicting business keys detected; sample={sample}")
        # Exact repeated snapshots collapse to one row; newest ingestion metadata wins.
        ordering = []
        if "_ingested_at" in valid.columns:
            ordering.append(F.col("_ingested_at").desc_nulls_last())
        if "_source_file" in valid.columns:
            ordering.append(F.col("_source_file").desc_nulls_last())
        if not ordering:
            ordering = [F.col("_business_hash").desc()]
        window = Window.partitionBy(*required).orderBy(*ordering)
        staged = (valid.withColumn("_row_num", F.row_number().over(window))
                  .filter(F.col("_row_num") == 1).drop("_row_num"))
        counts["accepted_rows"] = staged.count()
        target_table = f"{SILVER_DB}.{entity}"
        if spark.catalog.tableExists(target_table):
            target_df = spark.table(target_table)
            if set(target_df.columns) != set(staged.columns):
                raise ValueError(f"{target_table}: target schema columns differ from staged source. Resolve schema migration explicitly.")
            target = DeltaTable.forName(spark, target_table)
            join_expr = " AND ".join([f"t.`{k}` = s.`{k}`" for k in required])
            counts["updated_rows"] = (target.toDF().alias("t").join(staged.alias("s"),
                F.expr(join_expr), "inner").filter(F.col("t._business_hash") != F.col("s._business_hash")).count())
            counts["inserted_rows"] = (staged.alias("s").join(target.toDF().alias("t"),
                F.expr(join_expr), "left_anti").count())
            assignments = {c: f"s.`{c}`" for c in staged.columns}
            (target.alias("t").merge(staged.alias("s"), join_expr)
             .whenMatchedUpdate(condition="t._business_hash <> s._business_hash", set=assignments)
             .whenNotMatchedInsert(values=assignments).execute())
        else:
            counts["inserted_rows"] = counts["accepted_rows"]
            (staged.write.format("delta").mode("errorifexists").saveAsTable(target_table))
        append_audit(entity, "SUCCESS", counts, started_at)
        return {"entity": entity, "status": "SUCCESS", **counts}
    except Exception as exc:
        append_audit(entity, "FAILED", counts, started_at, str(exc)[:4000])
        raise

# COMMAND ----------

ensure_support_tables()
results = []
for entity_name in RAW_ENTITIES:
    try:
        result = process_entity(entity_name)
        results.append(result)
        print(result)
    except Exception as exc:
        results.append({"entity": entity_name, "status": "FAILED", "error": str(exc)[:1000]})
        print(f"FAILED: {entity_name}: {exc}")

display(spark.createDataFrame(results))

# COMMAND ----------

# Review the run-level audit after all entities have been attempted.
display(spark.table(AUDIT_TABLE).filter(F.col("run_id") == RUN_ID).orderBy("entity"))
display(spark.table(QUARANTINE_TABLE).filter(F.col("run_id") == RUN_ID).groupBy("entity", "reason").count())