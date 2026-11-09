# SQL Server deployment scripts

This folder contains the SQL Server objects used by the Azure Data Factory (ADF) metadata-driven ingestion pipelines and Databricks job orchestration logging.

These scripts are deployment/support scripts for the project's existing SQL database. They are **not** a complete fresh-database bootstrap because the original DDL for the existing ADF metadata tables is not included.

## Folder structure

```text
sql/
├── README.md
├── 01_schema/
│   ├── 01_adf_logging_support.sql
│   └── 02_databricks_job_log_table.sql
├── 02_constraints/
│   └── 01_log_status_constraint.sql
├── 03_stored_procedures/
│   ├── 01_child_logging.sql
│   ├── 02_master_logging.sql
│   ├── 03_master_job_metadata.sql
│   ├── 04_update_watermark.sql
│   └── 05_databricks_job_logging.sql
└── 04_validation/
    └── 01_validate_sql_objects.sql
```

## What each folder contains

### 01_schema

- `01_adf_logging_support.sql` — supporting objects and additive logging changes used by ADF, including the ADF run-ID mapping support.
- `02_databricks_job_log_table.sql` — table used to record Databricks job execution details.

### 02_constraints

- `01_log_status_constraint.sql` — allowed status values for the logging lifecycle.

### 03_stored_procedures

- `01_child_logging.sql` — start/end logging for child pipeline activity.
- `02_master_logging.sql` — start/end logging for the master pipeline.
- `03_master_job_metadata.sql` — retrieves metadata used by the metadata-driven master pipeline.
- `04_update_watermark.sql` — updates the source watermark after successful processing.
- `05_databricks_job_logging.sql` — records Databricks job execution details and outcome/error information.

### 04_validation

- `01_validate_sql_objects.sql` — checks that the expected project SQL objects exist after deployment.

## Existing prerequisite tables

The existing ADF metadata model depends on these tables:

- `dbo.ecom_tbl_trigger`
- `dbo.ecom_tbl_job`
- `dbo.ecom_tbl_job_dtls`
- `dbo.ecom_tbl_watermark`
- `dbo.ecom_tbl_log_dtls`

Their original full CREATE TABLE definitions are not included in this folder. The scripts assume these tables already exist and apply only the support objects or additive changes required by the current pipeline implementation.

Before deploying to a new database, export and review the actual table definitions from the existing SQL Server database. Do not create replacement tables based only on their names.

## Deployment order

Run the scripts against the project's SQL database (development database currently referred to as `sqldb-ecom-dev`) in this order:

1. `01_schema/01_adf_logging_support.sql`
2. `01_schema/02_databricks_job_log_table.sql`
3. `02_constraints/01_log_status_constraint.sql`
4. `03_stored_procedures/01_child_logging.sql`
5. `03_stored_procedures/02_master_logging.sql`
6. `03_stored_procedures/03_master_job_metadata.sql`
7. `03_stored_procedures/04_update_watermark.sql`
8. `03_stored_procedures/05_databricks_job_logging.sql`
9. `04_validation/01_validate_sql_objects.sql`

Run in a non-production environment first. Review each script before execution and confirm the target database. Some scripts use guarded object creation or `CREATE OR ALTER PROCEDURE` so they can be rerun, but rerunnable does not mean risk-free.

## Important implementation notes

- The existing pipeline logging design uses a numeric `master_run_id`; ADF pipeline run IDs are mapped through `dbo.ecom_adf_run_map`.
- Databricks workspace job IDs can exceed the SQL Server `INT` range. Store/pass them using `BIGINT` in SQL and use a compatible string or 64-bit parameter type in ADF. Do not map a Databricks job ID to `Int32`.
- Keep the Databricks job ID separate from the ADF pipeline run ID. They identify different executions.
- Error logging should capture the failed activity/procedure context and the actual error message while avoiding credentials or other sensitive values.
- The ADF master pipeline invokes the Databricks processing pipeline; the Databricks job itself manages the Bronze → Silver → Gold → Data Quality task dependencies.
- Do not run obsolete scripts from the former flat SQL folder layout in addition to these organized scripts unless you have checked for duplicate object changes.

## Post-deployment validation

1. Run `04_validation/01_validate_sql_objects.sql`.
2. Confirm all prerequisite tables and expected support objects exist in the target database.
3. Confirm the stored procedures compile successfully.
4. Run a controlled ADF test and inspect both successful and failed activity logging.
5. Confirm Databricks job IDs are preserved without integer overflow and that the failure message is populated for a forced test failure only in a non-production environment.

## Security

- Do not commit database credentials, connection strings, tokens or secrets.
- Use the project's approved Key Vault-backed credential mechanism.
- Restrict write/execute permissions to the identities and operators that need them.
