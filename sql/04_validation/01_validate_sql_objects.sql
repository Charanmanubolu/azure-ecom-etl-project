/* Run after deploying the SQL scripts. */
SET NOCOUNT ON;

SELECT t.name AS TableName
FROM sys.tables t
WHERE t.name IN ('ecom_adf_run_map','ecom_databricks_job_log','ecom_tbl_log_dtls')
ORDER BY t.name;

SELECT s.name AS SchemaName, p.name AS ProcedureName
FROM sys.procedures p
JOIN sys.schemas s ON s.schema_id=p.schema_id
WHERE p.name IN (
 'ecom_start_master_log','ecom_end_master_log',
 'ecom_start_log_entry','ecom_end_log_entry',
 'ecom_get_master_job_metadata','ecom_update_watermark',
 'ecom_start_databricks_log','ecom_end_databricks_log'
)
ORDER BY p.name;

SELECT name, definition
FROM sys.check_constraints
WHERE parent_object_id=OBJECT_ID(N'dbo.ecom_tbl_log_dtls')
 AND name=N'CK_ecom_tbl_log_dtls_job_status';
