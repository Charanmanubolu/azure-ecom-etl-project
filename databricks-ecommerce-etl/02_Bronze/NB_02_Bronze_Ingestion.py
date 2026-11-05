# Databricks notebook source
# MAGIC %run ../01_Configuration/01_Project_Config

# COMMAND ----------

from datetime import datetime, timezone
from pyspark.sql import functions as F

LOAD_MODE = "overwrite"  # Initial full load only

BATCH_ID = datetime.now(timezone.utc).strftime(
    "%Y%m%dT%H%M%S"
)

print("Batch ID:", BATCH_ID)
print("Environment:", ENVIRONMENT)
print("Catalog:", UC_CATALOG)
print("Entities:", len(RAW_ENTITIES))

# COMMAND ----------

def load_raw_to_bronze(entity):

    source_path = RAW_ENTITY_PATHS[entity]
    target_path = BRONZE_ENTITY_PATHS[entity]

    target_table = f"{BRONZE_DB}.{entity}"

    print(f"\nStarting ingestion: {entity}")
    print("Source:", source_path)
    print("Target:", target_path)

    # Read all dated Parquet folders recursively
    df = (
        spark.read
        .format("parquet")
        .option("recursiveFileLookup", "true")
        .load(source_path)
    )

    source_count = df.count()

    if source_count == 0:
        raise ValueError(f"No records found for {entity}")

    # Add ingestion metadata
    df = (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_batch_id", F.lit(BATCH_ID))
    )

    # Write Bronze Delta data
    (
        df.write
        .format("delta")
        .mode(LOAD_MODE)
        .option("overwriteSchema", "true")
        .save(target_path)
    )

    # Register external Delta table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {target_table}
        USING DELTA
        LOCATION '{target_path}'
    """)

    # Validate target count
    target_count = (
        spark.read
        .format("delta")
        .load(target_path)
        .count()
    )

    if source_count != target_count:
        raise ValueError(
            f"Count mismatch for {entity}: "
            f"source={source_count}, target={target_count}"
        )

    print(f"SUCCESS: {entity}")
    print("Source count:", source_count)
    print("Target count:", target_count)

    return {
        "entity": entity,
        "status": "SUCCESS",
        "source_count": source_count,
        "target_count": target_count,
        "target_table": target_table
    }

# COMMAND ----------

results = []

for entity in RAW_ENTITIES:

    try:
        result = load_raw_to_bronze(entity)
        results.append(result)

    except Exception as e:

        print(f"FAILED: {entity}")
        print(str(e))

        results.append({
            "entity": entity,
            "status": "FAILED",
            "source_count": None,
            "target_count": None,
            "target_table": f"{BRONZE_DB}.{entity}",
            "error": str(e)[:1000]
        })

# COMMAND ----------

results_df = spark.createDataFrame(results)

display(results_df)

success_count = sum(
    1 for r in results if r["status"] == "SUCCESS"
)

failure_count = sum(
    1 for r in results if r["status"] == "FAILED"
)

print("Total entities:", len(RAW_ENTITIES))
print("Successful:", success_count)
print("Failed:", failure_count)