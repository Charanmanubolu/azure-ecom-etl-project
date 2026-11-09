# Azure E-commerce ETL Project

An end-to-end data engineering project built with **Azure Data Factory, Azure Data Lake Storage Gen2, Azure Databricks, Azure SQL Server and Databricks SQL dashboards**. It uses the Brazilian Olist e-commerce dataset and a Medallion architecture to turn landed source files into analytics-ready data.

The repository's default working branch is `main`.

## 1. Solution overview

### Architecture

```text
Source data
    |
    v
Azure Data Factory
(metadata-driven ingestion pipelines)
    |
    v
ADLS Gen2 - raw
    |
    v
Databricks Bronze
(Auto Loader -> Delta)
    |
    v
Databricks Silver
(cleaning, standardization, deduplication, MERGE)
    |
    v
Databricks Gold
(dimensions and fact tables)
    |
    v
Data Quality checks
    |
    v
Databricks SQL Dashboard
(Ecommerce Sales Analytics)
```

ADF handles source ingestion and starts the Databricks ETL workflow. The Databricks job manages dependencies between the notebook tasks. The Gold layer is the reporting layer used by the dashboard.

## 2. Technology stack

- **Azure Data Factory** — metadata-driven ingestion, master orchestration and operational logging.
- **Azure Data Lake Storage Gen2** — raw and curated storage.
- **Azure Databricks** — PySpark, Spark SQL, Delta Lake and Auto Loader.
- **Unity Catalog** — catalog and schema governance.
- **Azure SQL Database / SQL Server** — ADF metadata and pipeline logging objects.
- **Databricks SQL** — dashboard queries and visualization.
- **GitHub** — source control and project versioning.

## 3. Data lake and catalog

The project uses the following logical storage layout:

```text
ADLS Gen2
├── raw/
│   └── <entity>/YYYY/MM/DD/
└── curated/
    ├── bronze/
    │   └── <entity>/
    ├── silver/
    ├── gold/
    └── _autoloader/
        └── bronze/
            ├── checkpoints/
            └── schemas/
```

Unity Catalog:

- Catalog: `adb_ecom_dev_01`
- Schemas: `bronze`, `silver`, `gold`

Raw source files and Bronze Delta tables are separate layers. Auto Loader schema and checkpoint state are stored separately from the Bronze data.

## 4. Databricks ETL workflow

The Databricks workflow is orchestrated by the job `JOB_ECOM_MASTER_ETL`.

```text
NB_Bronze_Ingestion
        |
NB_Silver_Transformation
        |
   +----+----+
   |         |
NB_Gold_   NB_Gold_
Dimensions Facts
   |         |
   +----+----+
        |
 NB_Data_Quality
```

The Gold dimensions and facts run after Silver and can run in parallel. Data Quality runs after both Gold tasks complete.

### Layer responsibilities

| Layer | Responsibility |
|---|---|
| Bronze | Ingest raw Parquet files into Delta using Auto Loader and retain source/audit metadata. |
| Silver | Standardize schemas and data types, remove invalid or duplicate records, validate business keys and maintain tables with Delta MERGE. |
| SCD Type 1 | Apply overwrite-style updates where historical attribute versions are not required. |
| Gold dimensions | Build conformed dimensions for customer, product, seller and date analysis. |
| Gold facts | Build order, order-item, payment and review fact tables using dimension keys. |
| Data Quality | Validate keys, duplicates, relationships and business rules; persist results and fail the task when configured checks fail. |

### Gold tables

Schema: `adb_ecom_dev_01.gold`

**Dimensions**
- `dim_customer`
- `dim_product`
- `dim_seller`
- `dim_date`

**Facts**
- `fact_orders`
- `fact_order_items`
- `fact_order_payments`
- `fact_order_reviews`

Fact-to-dimension relationships use deterministic surrogate keys. Fact construction uses left joins so unmatched dimension records can be identified by data-quality checks instead of being silently dropped.

### SCD scope

SCD Type 1 is included in the project. SCD Type 2 is not part of the current project scope; historical version tracking is not required for the agreed deliverable. The Type 2 notebook file is retained as a placeholder/reference and should not be treated as an implemented workflow.

## 5. Azure Data Factory

ADF contains the metadata-driven ingestion master pipeline and source-specific child pipelines. The Databricks processing pipeline is:

- `PL_Databricks_Ecom_ETL`

It invokes the Databricks job:

- `JOB_ECOM_MASTER_ETL`

The ADF master pipeline is:

- `PL1_Master`

The ADF logging SQL objects are maintained separately in the `sql/` directory. ADF is responsible for starting the Databricks job and waiting for its completion; notebook-level task dependencies are maintained in the Databricks job.

## 6. SQL Server support objects

The `sql/` directory contains separately organized deployment scripts:

```text
sql/
├── README.md
├── 01_schema/
├── 02_constraints/
├── 03_stored_procedures/
└── 04_validation/
```

These scripts support ADF master/child execution logging, metadata lookup, watermark handling and Databricks job execution logging.

Read [sql/README.md](sql/README.md) before deploying. The existing metadata tables are prerequisites; their original CREATE TABLE definitions are not included in this script set. Do not treat the folder as a clean SQL Server database bootstrap.

## 7. Repository structure

```text
.
├── databricks-ecommerce-etl/
│   ├── 00_Setup/
│   ├── 01_Configuration/
│   ├── 02_Bronze/
│   ├── 03_Silver/
│   ├── 04_SCD/
│   ├── 05_Gold/
│   ├── 06_Data_Quality/
│   ├── 99_Utilities/
│   └── Ecommerce Sales Analytics.lvdash.json
├── pipeline/
├── dataset/
├── linkedService/
├── factory/
├── integrationRuntime/
├── sql/
└── README.md
```

ADF assets are stored as JSON exports in their respective folders. The Databricks dashboard definition is stored as a Lakeview dashboard JSON file. Dashboard deployment and data-source permissions still need to be configured in the target workspace.

## 8. Dashboard

**Ecommerce Sales Analytics** is a Databricks SQL dashboard built on the Gold layer. It includes filters and KPI/trend visualizations such as:

- Order date
- Customer state
- Product category
- Payment type
- Total orders
- Total revenue
- Total customers
- Average order value
- Average review score
- Monthly revenue and order trends

Dashboard source file:

`databricks-ecommerce-etl/Ecommerce Sales Analytics.lvdash.json`

## 9. Running the project

Use this high-level execution order:

1. Confirm the ADF linked services, datasets, integration runtime and SQL logging objects are configured.
2. Run or trigger `PL1_Master` for source ingestion when a fresh raw load is required.
3. Run `PL_Databricks_Ecom_ETL` from ADF, or run `JOB_ECOM_MASTER_ETL` directly in Databricks for Databricks-only testing.
4. Confirm each task succeeds in this order: Bronze, Silver, Gold Dimensions and Gold Facts, then Data Quality.
5. Confirm the expected Gold tables and data-quality results exist in Unity Catalog.
6. Refresh the Databricks SQL dashboard and verify its filters and visualizations.

For a rerun, use the existing checkpoints and Delta tables. Do not delete checkpoints or truncate Bronze/Silver/Gold tables unless the impact and recovery procedure are understood.

## 10. Validation notes and known considerations

- The user has confirmed successful Databricks execution of Bronze, Silver, Gold dimensions, Gold facts and Data Quality.
- Silver row counts last verified by the user:
  - `orders`: 99,441
  - `order_items`: 112,650
  - `order_payments`: 103,886
  - `order_reviews`: 99,224
- The Bronze ingestion logic supports changed source files being reprocessed when the same source path is overwritten. Preserve the associated source modification-time handling and checkpoint behavior.
- Repeated full snapshots can contain the same business records in different files. File-level ingestion protection is not a substitute for Silver business-key deduplication.
- Confirm the latest ADF master run and SQL logging output in Azure before calling the full ADF-to-Databricks execution production-ready.
- Validate dashboard permissions and refresh behavior in the target Databricks workspace.

## 11. Security and operational practices

1. Never commit passwords, connection strings, access tokens, client secrets or private keys.
2. Use the approved Key Vault/secret-scope integration for credentials.
3. Do not change shared linked services, integration runtimes, checkpoints or production data paths without checking dependencies.
4. Keep transformations idempotent and validate business keys before MERGE.
5. Review failed ADF activities and stored-procedure error details; do not log secrets.
6. Keep development and production configuration separate.
7. Treat GitHub as the source of truth for project code and exported configuration. Verify workspace deployment separately.

## 12. Current project status

| Component | Status |
|---|---|
| ADLS Gen2 and Unity Catalog foundation | Configured |
| ADF source ingestion pipelines | Implemented |
| ADF SQL logging support | Scripts organized; deployment and latest run should be verified |
| Databricks Bronze | Implemented and run successfully |
| Databricks Silver | Implemented and run successfully |
| SCD Type 1 | Included |
| SCD Type 2 | Out of scope |
| Gold dimensions and facts | Implemented and run successfully |
| Data Quality | Implemented and run successfully |
| Databricks SQL dashboard | Created; workspace refresh/permissions should be verified |

## 13. License and dataset

The project uses the Brazilian Olist e-commerce public dataset for learning and demonstration. Refer to the dataset provider's terms and attribution requirements when redistributing source data.

---

**Repository:** [Charanmanubolu/azure-ecom-etl-project](https://github.com/Charanmanubolu/azure-ecom-etl-project)  
**Working branch:** `main`
