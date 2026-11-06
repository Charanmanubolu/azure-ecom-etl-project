# Databricks notebook source
# MAGIC %run ../01_Configuration/01_Project_Config

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.sql import functions as F

# Auto Loader settings
INCLUDE_EXISTING_FILES = True
SCHEMA_EVOLUTION_MODE = "addNewColumns"

BATCH_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid4().hex[:8]

# Keep Auto Loader state outside the Bronze Delta table folders.
AUTOLOADER_ROOT = f"{CURATED_ROOT}_autoloader/bronze/"

print("Batch ID:", BATCH_ID)
print("Environment:", ENVIRONMENT)
print("Catalog:", UC_CATALOG)
print("Entities:", len(RAW_ENTITIES))
print("Include existing raw files:", INCLUDE_EXISTING_FILES)

# COMMAND ----------

def load_raw_to_bronze(entity):
    """
    Incrementally ingest new raw Parquet files with Auto Loader.

    The Auto Loader checkpoint tracks discovered files. The foreachBatch
    write also checks _source_file in Bronze so a retry after a successful
    Delta write but before checkpoint commit does not append the same file
    again.
    """

    source_path = RAW_ENTITY_PATHS[entity]
    target_path = BRONZE_ENTITY_PATHS[entity]
    target_table = f"{BRONZE_DB}.{entity}"

    schema_path = f"{AUTOLOADER_ROOT}schemas/{entity}/"
    checkpoint_path = f"{AUTOLOADER_ROOT}checkpoints/{entity}/"

    print(f"\nStarting Auto Loader ingestion: {entity}")
    print("Source:", source_path)
    print("Target:", target_path)
    print("Schema location:", schema_path)
    print("Checkpoint:", checkpoint_path)

    batch_metrics = {
        "rows_written": 0,
        "files_seen": 0,
        "files_written": 0
    }

    def write_new_files(micro_batch_df, micro_batch_id):
        if micro_batch_df.isEmpty():
            return

        # One source file is the idempotency unit for this raw-to-Bronze flow.
        incoming_files = (
            micro_batch_df
            .select("_source_file")
            .distinct()
        )
        batch_metrics["files_seen"] += incoming_files.count()

        if DeltaTable.isDeltaTable(spark, target_path):
            existing_bronze = spark.read.format("delta").load(target_path)

            if "_source_file" not in existing_bronze.columns:
                raise ValueError(
                    f"Existing Bronze data for {entity} does not contain "
                    "_source_file. Resolve the existing table before enabling "
                    "Auto Loader to avoid duplicate ingestion."
                )

            existing_files = (
                existing_bronze
                .select("_source_file")
                .distinct()
            )

            new_rows = micro_batch_df.join(
                existing_files,
                on="_source_file",
                how="left_anti"
            )
        else:
            new_rows = micro_batch_df

        new_file_count = new_rows.select("_source_file").distinct().count()
        new_row_count = new_rows.count()

        if new_row_count == 0:
            print(
                f"Micro-batch {micro_batch_id}: no new files to append."
            )
            return

        (
            new_rows.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(target_path)
        )

        batch_metrics["rows_written"] += new_row_count
        batch_metrics["files_written"] += new_file_count

        print(
            f"Micro-batch {micro_batch_id}: "
            f"appended {new_row_count} rows from {new_file_count} new files."
        )

    # Read Parquet files incrementally. Auto Loader persists discovery state
    # in the per-entity checkpoint directory.
    source_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.includeExistingFiles", str(INCLUDE_EXISTING_FILES).lower())
        .option("cloudFiles.schemaEvolutionMode", SCHEMA_EVOLUTION_MODE)
        .load(source_path)
    )

    enriched_stream = (
        source_stream
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn(
            "_source_file_modification_time",
            F.col("_metadata.file_modification_time")
        )
        .withColumn("_batch_id", F.lit(BATCH_ID))
    )

    query = (
        enriched_stream.writeStream
        .foreachBatch(write_new_files)
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()

    # Register the external Delta table after the first successful write.
    if DeltaTable.isDeltaTable(spark, target_path):
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {target_table}
            USING DELTA
            LOCATION '{target_path}'
        """)
    else:
        print(f"No new data found for {entity}; Bronze table was not changed.")

    print(f"SUCCESS: {entity}")
    print("Rows appended:", batch_metrics["rows_written"])
    print("New files appended:", batch_metrics["files_written"])
    print("Files encountered:", batch_metrics["files_seen"])

    return {
        "entity": entity,
        "status": "SUCCESS",
        "rows_written": batch_metrics["rows_written"],
        "files_written": batch_metrics["files_written"],
        "files_seen": batch_metrics["files_seen"],
        "target_table": target_table
    }

# COMMAND ----------

results = []

for entity in RAW_ENTITIES:
    try:
        results.append(load_raw_to_bronze(entity))

    except Exception as e:
        print(f"FAILED: {entity}")
        print(str(e))

        results.append({
            "entity": entity,
            "status": "FAILED",
            "rows_written": 0,
            "files_written": 0,
            "files_seen": 0,
            "target_table": f"{BRONZE_DB}.{entity}",
            "error": str(e)[:1000]
        })

# COMMAND ----------

results_df = spark.createDataFrame(results)
display(results_df)

success_count = sum(1 for r in results if r["status"] == "SUCCESS")
failure_count = sum(1 for r in results if r["status"] == "FAILED")

print("Total entities:", len(RAW_ENTITIES))
print("Successful:", success_count)
print("Failed:", failure_count)
