# SQL Server scripts

This folder contains the SQL Server objects used by the Azure Data Factory metadata-driven ingestion and Databricks orchestration.

## Layout

- `01_schema/`: support tables, sequence, and additive changes to the existing logging table.
- `02_constraints/`: allowed logging statuses.
- `03_stored_procedures/`: child/master logging, metadata lookup, watermark update, and Databricks execution logging.
- `04_validation/`: post-deployment checks.

## Deployment order

Run these scripts against the project SQL database (currently `sqldb-ecom-dev`) in this order:

1. `01_schema/01_adf_logging_support.sql`
2. `01_schema/02_databricks_job_log_table.sql`
3. `02_constraints/01_log_status_constraint.sql`
4. `03_stored_procedures/01_child_logging.sql`
5. `03_stored_procedures/02_master_logging.sql`
6. `03_stored_procedures/03_master_job_metadata.sql`
7. `03_stored_procedures/04_update_watermark.sql`
8. `03_stored_procedures/05_databricks_job_logging.sql`
9. `04_validation/01_validate_sql_objects.sql`

Scripts use `CREATE OR ALTER PROCEDURE` and guarded table/column creation where applicable, so they can be re-run for deployment updates.

## Existing prerequisite tables

The current ADF metadata model already relies on these tables:

- `dbo.ecom_tbl_trigger`
- `dbo.ecom_tbl_job`
- `dbo.ecom_tbl_job_dtls`
- `dbo.ecom_tbl_watermark`
- `dbo.ecom_tbl_log_dtls`

The repository did not contain their original full CREATE TABLE definitions when this folder was reorganized. This deployment therefore treats them as pre-existing project tables and does not guess their column definitions. The scripts add only the logging columns/indexes required by the current pipelines. If you want a full clean-database bootstrap script, export the original table DDL from SQL Server and add it separately before these migrations.

## Important

- Keep the numeric `master_run_id` used by the existing log table. ADF RunIds are mapped through `dbo.ecom_adf_run_map`.
- The Databricks workspace job ID is stored as `BIGINT` in `dbo.ecom_databricks_job_log`.
- The Databricks child pipeline must pass its job ID as a string to ADF's SQL stored-procedure activity only if the parameter mapping is configured as String; the SQL procedure accepts BIGINT.
- Do not run old numbered patch scripts from the former flat layout after deploying these organized scripts. They are retained in Git history, not duplicated here.
