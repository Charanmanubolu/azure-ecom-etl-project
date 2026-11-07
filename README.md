# Azure E-commerce ETL Project

An end-to-end Azure data engineering project using the Brazilian Olist e-commerce dataset. The platform lands source extracts in Azure Data Lake Storage Gen2 (ADLS Gen2), ingests them into Bronze Delta, transforms them into Silver and Gold data products with Azure Databricks, orchestrates ingestion with Azure Data Factory (ADF), and is intended to support Power BI reporting.

> **Implementation note:** This README distinguishes confirmed implementation from planned work and outstanding validation. A design is not marked complete merely because a notebook or configuration file exists.

## 1. Project goals

- Ingest e-commerce entities from the source systems into ADLS Gen2.
- Preserve landed source files in a date-partitioned raw zone.
- Ingest raw Parquet files into Bronze Delta using Databricks Auto Loader.
- Clean, standardize, deduplicate and incrementally maintain Silver Delta tables.
- Apply SCD Type 1 or Type 2 only to dimensions for which the history requirement is agreed.
- Build Gold facts, dimensions and business aggregates for analytics.
- Orchestrate the end-to-end workflow with ADF and Databricks Workflows.
- Add data-quality checks, operational logging and project documentation.
- Deliver reporting datasets for Power BI.

## 2. Azure environment

| Resource | Confirmed project setting |
|---|---|
| Resource group | `RG-ECOM-ETL-DEV-CIN` |
| Azure region | Central India |
| Databricks workspace | `adb-ecom-dev-01` |
| Databricks tier | Premium |
| Databricks network mode | Secure Cluster Connectivity / No Public IP |
| ADLS Gen2 account | `stecraw001` |
| ADLS containers | `raw`, `curated` |
| Unity Catalog catalog | `adb_ecom_dev_01` |
| Unity Catalog schemas | `bronze`, `silver`, `gold` |
| ADF | Existing project factory; master/child orchestration and logging have been implemented and tested |
| GitHub repository | `Charanmanubolu/azure-ecom-etl-project`, branch `main` |

Other project resources include Azure SQL Database, Key Vault and a Self-hosted Integration Runtime. Their exact connection settings and remaining deployment dependencies must be verified in Azure before production use. Never commit connection strings, passwords, tokens, client secrets or other credentials.

### Databricks decisions already agreed

- Unity Catalog governance.
- Hybrid storage design.
- Bronze tables are external Delta tables.
- Silver and Gold tables are managed Delta tables.
- PySpark and Spark SQL for transformations.
- Development compute is manually started/stopped and auto-terminates when idle.
- ADF is the master orchestrator; Databricks Workflows manage dependencies between Databricks tasks.
- ADF starts one Databricks Job and waits for its completion.
- Bronze ingestion uses Auto Loader with `trigger(availableNow=True)`.
- Silver uses Delta MERGE for insert/update processing where appropriate.
- Gold uses incremental MERGE or targeted refresh where appropriate; a full refresh may be used for small/simple outputs.
- Delta Change Data Feed may be used where it adds value; it is not assumed to be enabled.

## 3. Storage layout

The agreed ADLS layout is:

```text
stecraw001/
├── input/
├── raw/                         # ADF-landed source Parquet files
│   └── <entity>/YYYY/MM/DD/
├── curated/
│   ├── bronze/                  # External Delta data
│   │   └── <entity>/
│   ├── silver/                  # Managed Delta table storage
│   ├── gold/                    # Managed Delta table storage
│   └── _autoloader/
│       └── bronze/
│           ├── checkpoints/
│           │   └── <entity>/
│           └── schemas/
│               └── <entity>/
└── backup/
```

Important distinctions:

- `raw/` contains source Parquet files; it is not the Bronze Delta layer.
- `curated/bronze/<entity>/` contains Bronze Delta data.
- Auto Loader schema and checkpoint state are stored separately under `curated/_autoloader/bronze/`.
- Unity Catalog external locations are rooted at the curated container. Entity/table paths are child paths; do not redefine the external-location root as an entity folder.

## 4. Source entities

The raw entity folders currently configured in the project are:

`category_translation`, `customer`, `geolocation`, `jsontest`, `order_items`, `order_payments`, `order_reviews`, `orders`, `product_category_name_translation`, `products`, and `sellers`.

The original project presentation describes multiple source types and routes (SQL Server, REST API, Snowflake, bulk ADLS, SFTP, CSV, Excel and JSON). The current observed ADLS raw landing files and Bronze tables are Parquet. These are different layers of the architecture: the original source system/format and the ADF landing output format must not be conflated.

The `jsontest` entity is a small sample JSON dataset and is not currently established as part of the Olist business model.

## 5. Implemented stages and current status

### Stage 1 — Azure foundation and governance

**Status: configured; validate deployment details before final release.**

- Development resource group and Central India region selected.
- Databricks workspace and ADLS Gen2 account configured.
- Unity Catalog catalog `adb_ecom_dev_01` and schemas `bronze`, `silver`, `gold` created.
- External locations `extloc_ecom_raw_dev` and `extloc_ecom_curated_dev` configured.
- Required storage permissions were tested, including file read/write and external table creation.
- Development compute settings were agreed: Standard access mode, Runtime 18 LTS, single-node Standard_D4ds_v5, Photon disabled initially, 20-minute auto-termination, manual start/stop.

### Stage 2 — ADF ingestion and orchestration

**Status: master/child orchestration and logging implemented and tested; source completeness and final integration remain under validation.**

- ADF is the master orchestrator.
- Existing master/child pipeline design and logging should be preserved.
- Project pipeline exports currently available in the working materials include REST-to-raw, SQL-to-raw and SFTP-to-raw pipeline JSON, plus REST linked-service/dataset JSON.
- Raw landing convention: `raw/<entity>/YYYY/MM/DD/<filename>.parquet`.
- ADF ingestion currently produces Parquet landing files, including when the upstream source format differs.
- Restart-safe landing is based on creating a new dated file rather than overwriting an existing landed file.

**Known source-data issue — orders:** The current Bronze `orders` table contains only 329 rows from one file at `raw/orders/2016/01/01/orders.parquet`. This is not established as the complete Olist orders dataset. The source table, ADF query/mapping and file selection must be checked before orders can be accepted as complete.

### Stage 3 — Databricks configuration

**Status: configured.**

Configuration notebook:

`databricks-ecommerce-etl/01_Configuration/01_Project_Config.ipynb`

It defines the development environment, catalog/schema names, ADLS roots, raw entity list, raw/curated path helpers, Bronze entity paths and shared audit-column names.

### Stage 4 — Bronze ingestion

**Status: implemented and successfully tested for all 11 configured entities.**

Notebook:

`databricks-ecommerce-etl/02_Bronze/NB_02_Bronze_Ingestion.py`

Implementation details:

- Reads Parquet from raw entity folders using Auto Loader (`cloudFiles`).
- Uses separate schema and checkpoint directories for each entity.
- Uses `includeExistingFiles=True`, schema evolution mode `addNewColumns` and `availableNow=True`.
- Enriches rows with `_ingested_at`, `_source_file`, `_source_file_modification_time` and `_batch_id` where available.
- Uses `foreachBatch` and an anti-join against already-landed source file paths to protect against re-appending the same file after a retry.
- Appends to external Delta paths under `curated/bronze/<entity>/`.
- Registers successful Delta outputs in Unity Catalog as `adb_ecom_dev_01.bronze.<entity>`.
- All 11 configured Bronze tables were confirmed registered.

**Important limitation:** File-level duplicate protection prevents reprocessing the same file path. It does not deduplicate the same business records arriving in different dated full-snapshot files. A changed file reusing the same path would also be skipped by the current file-path guard. Do not delete/reset Bronze data or Auto Loader checkpoints as a casual troubleshooting step.

### Stage 5 — Silver transformations

**Status: design/inspection in progress; implementation not yet complete.**

Placeholder notebook:

`databricks-ecommerce-etl/03_Silver/NB_03_Silver_Transformation.sql`

The notebook currently contains only the Databricks notebook source marker. Actual Bronze schemas and counts have been inspected. Key findings:

| Entity | Observed Bronze rows | Distinct business-key count from inspection | Finding / proposed Silver handling |
|---|---:|---:|---|
| customer | 298,326 including 3 header rows | 99,441 after header removal | Three identical-sized snapshots; remove header rows, rename columns, deduplicate by `customer_id` |
| order_payments | 311,661 including 3 header rows | 103,886 after header removal | Three identical-sized snapshots; rename columns, cast measures, deduplicate by `order_id + payment_sequential` |
| order_reviews | 297,675 including 3 header rows | 98,410 review IDs | Three repeated snapshots; sample duplicate review IDs have identical business attributes; remove headers and deduplicate by `review_id` after validating conflicts |
| order_items | 337,950 | 112,650 | Three equal-sized snapshots; candidate key `order_id + order_item_id` |
| products | 98,853 | 32,951 | Three equal-sized snapshots; candidate key `product_id` |
| sellers | 9,285 | 3,095 | Three equal-sized snapshots; candidate key `seller_id` |
| orders | 329 | 329 | Only one small source file; hold from production Silver until completeness is resolved |
| category_translation | 142 | 71 distinct category mappings | Its 71 distinct mappings exactly match the other translation table |
| product_category_name_translation | 71 | 71 distinct category mappings | Same 71 mappings as `category_translation`; select one canonical Silver mapping |
| geolocation | 1,000,163 in one reported inspection | Not yet measured | String coordinates need numeric casting; multiple locations per ZIP may exist and need an agreed aggregation/retention rule |
| jsontest | 14 | Not yet measured | Keep separate from core Olist analytics unless a use case is agreed |

The repeated customer/payment/review/product/seller/order-item rows indicate repeated full snapshots, not true incremental source extracts. Silver must therefore be idempotent and must not multiply records each time the same snapshots are processed.

The candidate keys above are based on the observed source structure and must be checked for nulls and conflicting duplicate payloads before MERGE. For reviews, the sample indicates identical copies across snapshots, but a conflict check is still required.

### Stage 6 — SCD Type 1 / Type 2

**Status: reference material supplied; production implementation and entity selection not yet approved.**

- SCD Type 1 overwrites changed attributes and does not retain prior values.
- SCD Type 2 preserves history using version rows and active/effective-date attributes.
- The uploaded legacy examples use a different storage account and hard-coded credentials. They are reference material only and must not be copied into this project.
- The specific dimensions that require Type 2 history must be selected from business requirements.
- The supplied SCD2 sample requires correction before reuse: a matched changed row must be expired and a new version inserted; simply expiring the matched row in a MERGE does not by itself insert the changed version.

### Stage 7 — Gold, data quality and reporting

**Status: planned; design and implementation pending.**

The repository has folders reserved for Gold, SCD, data quality and utilities. The fact/dimension model, business KPI definitions, data-quality failure policy and Power BI semantic/reporting requirements must be agreed before implementation is marked complete.

## 6. Planned repository structure

```text
databricks-ecommerce-etl/
├── 00_Setup/
├── 01_Configuration/
├── 02_Bronze/
├── 03_Silver/
├── 04_SCD/
├── 05_Gold/
├── 06_Data_Quality/
└── 99_Utilities/
```

The current repository contents should be checked before adding files; the structure above is the project organization agreed so far, not a claim that every stage is implemented.

## 7. Engineering rules

1. Do not hard-code credentials or secrets. Use the project's approved Key Vault/secret-scope integration after its exact name and permissions are verified.
2. Do not change or delete existing Bronze Delta data, Auto Loader checkpoints, ADF pipelines, linked services or shared resources without checking dependencies and impact.
3. Do not infer schemas from names alone. Use observed Bronze schemas and sample records.
4. Validate business keys, null keys and conflicting duplicates before MERGE.
5. Keep transformations deterministic and rerunnable.
6. Preserve source lineage/audit metadata where it helps traceability.
7. Do not call a full-snapshot MERGE a true incremental source load. Document whether each task processes new files, a full snapshot or Delta changes.
8. Do not mark a stage complete until it has been run and its output validated in the target Azure environment.
9. Keep manual Azure actions and required approvals documented in this README.

## 8. Outstanding decisions and blockers

- Resolve why the orders source currently lands only 329 records.
- Confirm whether the dated source extracts are intended to be full snapshots or can be changed to true incremental extracts.
- Agree which business dimensions require SCD Type 2 history.
- Approve the Gold fact/dimension model and KPI definitions.
- Agree the handling/aggregation rule for multiple geolocation rows per ZIP code.
- Define the data-quality policy for rejected/quarantined records versus fail-fast behavior.
- Verify the approved Key Vault/secret-scope access path and rotate credentials exposed in legacy uploaded notebooks.
- Verify the current ADF pipeline assets, linked-service dependencies and Databricks Job/Workflow task wiring before changing orchestration.

## 9. Manual actions still required

These actions require access to the user's Azure/Databricks environment and cannot be completed by editing this repository alone:

1. Rotate/revoke the client credential exposed in the uploaded legacy SCD notebooks; check its service principal usage before disabling it.
2. Confirm the source SQL table and ADF extraction logic for orders, then rerun ingestion after correcting the source scope.
3. Verify the Key Vault-backed secret access configuration and permissions; do not paste secret values into notebooks or chat.
4. Run each updated Databricks notebook in the development workspace and share validation output before promoting the stage.
5. Configure/verify the Databricks Job and Workflow tasks, then connect the job to the existing ADF master pipeline without disturbing existing logging.
6. Validate the final Gold tables and Power BI connection/refresh in the target environment.

## 10. Change log

| Date | Change |
|---|---|
| 2026-11-07 | README rebuilt from the project presentation, uploaded assignments/reference materials, current repository files, confirmed Azure configuration and observed Bronze table diagnostics. Silver, SCD, Gold and data-quality stages are explicitly marked as pending or in design rather than complete. |
