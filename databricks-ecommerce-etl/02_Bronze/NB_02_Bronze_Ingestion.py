# Databricks notebook source
# MAGIC %run ../01_Configuration/01_Project_Config

# COMMAND ----------

from datetime import datetime, timezone
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.sql import functions as F

# Auto Loader configuration
INCLUDE_EXISTING_FILES = True
SCHEMA_EVOLUTION_MODE = "addNewColumns"

# Start with orders only. Expand this list after the first test succeeds.
ENTITIES_TO_PROCESS = RAW_ENTITIES

BATCH_ID = (
    datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    + "_"
    + uuid4().hex[:8]
)

# Checkpoint and schema state are kept separate from Bronze Delta table folders.
AUTOLOADER_ROOT = f"{CURATED_ROOT}_autoloader/bronze/"

print("Batch ID:", BATCH_ID)
print("Environment:", ENVIRONMENT)
print("Catalog:", UC_CATALOG)
print("Entities selected:", ENTITIES_TO_PROCESS)
print("Include existing raw files:", INCLUDE_EXISTING_FILES)

# COMMAND ----------

def load_raw_to_bronze(entity):
    """
    Incrementally ingest new raw Parquet files with Auto Loader.

    Auto Loader tracks discovered files in its checkpoint. Before appending,
    this function checks the existing Bronze Delta data by _source_file.
    This makes a retry safe if the Delta append succeeded but the streaming
    checkpoint was not committed.
    """

    if entity not in RAW_ENTITY_PATHS or entity not in BRONZE_ENTITY_PATHS:
        raise ValueError(f"Entity '{entity}' is missing from project configuration.")

    source_path = RAW_ENTITY_PATHS[entity]
    target_path = BRONZE_ENTITY_PATHS[entity]
    bronze_schema = BRONZE_SCHEMA
    target_table = f"{UC_CATALOG}.{BRONZE_SCHEMA}.{entity}"
    target_table_sql = f"`{entity}`"

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
            print(f"Micro-batch {micro_batch_id}: empty batch.")
            return

        incoming_files = (
            micro_batch_df
            .select("_source_file", "_source_file_modification_time")
            .where(F.col("_source_file").isNotNull())
            .distinct()
        )
        batch_metrics["files_seen"] += incoming_files.select("_source_file").distinct().count()

        if DeltaTable.isDeltaTable(spark, target_path):
            existing_bronze = spark.read.format("delta").load(target_path)

            if "_source_file" not in existing_bronze.columns:
                raise ValueError(
                    f"Existing Bronze Delta data for '{entity}' has no "
                    "'_source_file' column. Stop here and resolve the existing "
                    "table before enabling incremental ingestion."
                )

            if "_source_file_modification_time" not in existing_bronze.columns:
                existing_bronze = existing_bronze.withColumn(
                    "_source_file_modification_time",
                    F.lit(None).cast("timestamp")
                )

            existing_files = (
                existing_bronze
                .select("_source_file", "_source_file_modification_time")
                .where(F.col("_source_file").isNotNull())
                .distinct()
            )

            # If a landed file was overwritten at the same path, remove its
            # old Bronze rows so the updated file can replace them cleanly.
            changed_file_paths = (
                incoming_files.alias("new")
                .join(
                    existing_files.alias("old"),
                    on="_source_file",
                    how="inner"
                )
                .where(
                    ~F.col("new._source_file_modification_time").eqNullSafe(
                        F.col("old._source_file_modification_time")
                    )
                )
                .select(F.col("new._source_file").alias("_source_file"))
                .distinct()
            )
            changed_paths = [row["_source_file"] for row in changed_file_paths.collect()]

            if changed_paths:
                DeltaTable.forPath(spark, target_path).delete(
                    F.col("_source_file").isin(changed_paths)
                )
                print(
                    f"Micro-batch {micro_batch_id}: replacing changed source "
                    f"file(s): {changed_paths}"
                )

            # Refresh after deleting prior versions of overwritten files.
            existing_bronze = spark.read.format("delta").load(target_path)
            if "_source_file_modification_time" not in existing_bronze.columns:
                existing_bronze = existing_bronze.withColumn(
                    "_source_file_modification_time",
                    F.lit(None).cast("timestamp")
                )

            existing_files = (
                existing_bronze
                .select("_source_file", "_source_file_modification_time")
                .where(F.col("_source_file").isNotNull())
                .distinct()
            )

            new_file_versions = incoming_files.join(
                existing_files,
                on=["_source_file", "_source_file_modification_time"],
                how="left_anti"
            )
            new_rows = micro_batch_df.join(
                new_file_versions,
                on=["_source_file", "_source_file_modification_time"],
                how="inner"
            )
        else:
            new_rows = micro_batch_df

        new_file_count = new_rows.select("_source_file").distinct().count()
        new_row_count = new_rows.count()

        if new_row_count == 0:
            print(
                f"Micro-batch {micro_batch_id}: no new or changed file versions "
                "to append."
            )
            return

        # Append new files and replacement versions of overwritten files.
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
            f"Micro-batch {micro_batch_id}: appended {new_row_count} rows "
            f"from {new_file_count} new files."
        )

    source_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        # Reprocess a source file if ADF overwrites it at the same path.
        .option("cloudFiles.allowOverwrites", "true")
        .option("cloudFiles.schemaLocation", schema_path)
        .option(
            "cloudFiles.includeExistingFiles",
            str(INCLUDE_EXISTING_FILES).lower()
        )
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

    # Table registration is handled in the dedicated cell after ingestion.

    print(f"Completed: {entity}")
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

for entity in ENTITIES_TO_PROCESS:
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
            "target_table": f"{UC_CATALOG}.{BRONZE_SCHEMA}.{entity}",
            "error": str(e)[:1000]
        })

# COMMAND ----------


# Register successful Bronze Delta outputs in Unity Catalog.
# This cell is intentionally self-contained; it does not depend on function-local variables.

bronze_schema = BRONZE_SCHEMA
spark.sql(f"USE CATALOG `{UC_CATALOG}`")
spark.sql(f"USE SCHEMA `{BRONZE_SCHEMA}`")

registration_results = []

for result in results:
    if result["status"] != "SUCCESS":
        continue

    entity = result["entity"]
    target_path = BRONZE_ENTITY_PATHS[entity]

    if DeltaTable.isDeltaTable(spark, target_path):
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS `{entity}`
            USING DELTA
            LOCATION '{target_path}'
        """)
        registration_results.append({
            "entity": entity,
            "table": f"{UC_CATALOG}.{bronze_schema}.{entity}",
            "status": "REGISTERED"
        })
        print(f"Registered table: {UC_CATALOG}.{bronze_schema}.{entity}")
    else:
        registration_results.append({
            "entity": entity,
            "table": f"{UC_CATALOG}.{bronze_schema}.{entity}",
            "status": "NO_DELTA_DATA"
        })
        print(f"No Bronze Delta data exists yet for {entity}.")

if registration_results:
    display(spark.createDataFrame(registration_results))
else:
    print("No successful Bronze tables available for registration.")

# COMMAND ----------

if registration_results:
    display(spark.createDataFrame(registration_results))
else:
    print("No successful Bronze tables available for registration.")
    
success_count = sum(1 for r in results if r["status"] == "SUCCESS")
failure_count = sum(1 for r in results if r["status"] == "FAILED")

print("Total entities selected:", len(ENTITIES_TO_PROCESS))
print("Successful:", success_count)
print("Failed:", failure_count)