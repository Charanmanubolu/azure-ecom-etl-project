/* Additive support schema for ADF run logging. Existing metadata/log tables are prerequisites. */
SET XACT_ABORT ON;
GO

/*
 Azure E-commerce ETL - SQL contract for the existing ADF JSON.
 No ADF pipeline activities, datasets, or linked services are changed.
 Existing BIGINT master_run_id and sequence are preserved. ADF GUIDs are
 mapped to numeric IDs so parallel ForEach jobs share one master execution.
*/
SET XACT_ABORT ON;
GO

IF COL_LENGTH('dbo.ecom_tbl_log_dtls','master_pipeline_id') IS NULL
 ALTER TABLE dbo.ecom_tbl_log_dtls ADD master_pipeline_id VARCHAR(100) NULL;
IF COL_LENGTH('dbo.ecom_tbl_log_dtls','activity_name') IS NULL
 ALTER TABLE dbo.ecom_tbl_log_dtls ADD activity_name VARCHAR(200) NULL;
IF COL_LENGTH('dbo.ecom_tbl_log_dtls','error_code') IS NULL
 ALTER TABLE dbo.ecom_tbl_log_dtls ADD error_code VARCHAR(100) NULL;
IF COL_LENGTH('dbo.ecom_tbl_log_dtls','child_run_id') IS NULL
 ALTER TABLE dbo.ecom_tbl_log_dtls ADD child_run_id VARCHAR(100) NULL;
IF COL_LENGTH('dbo.ecom_tbl_log_dtls','error_details') IS NULL
 ALTER TABLE dbo.ecom_tbl_log_dtls ADD error_details VARCHAR(MAX) NULL;
GO

IF OBJECT_ID('dbo.Seq_ecom_master_run_id','SO') IS NULL
 EXEC('CREATE SEQUENCE dbo.Seq_ecom_master_run_id AS BIGINT START WITH 1 INCREMENT BY 1;');
GO

IF OBJECT_ID('dbo.ecom_adf_run_map','U') IS NULL
BEGIN
 CREATE TABLE dbo.ecom_adf_run_map
 (
  adf_master_run_id VARCHAR(100) NOT NULL CONSTRAINT PK_ecom_adf_run_map PRIMARY KEY,
  master_run_id BIGINT NOT NULL CONSTRAINT UQ_ecom_adf_run_map_master_run UNIQUE,
  master_pipeline_id VARCHAR(100) NOT NULL,
  created_date DATETIME2(7) NOT NULL CONSTRAINT DF_ecom_adf_run_map_created DEFAULT SYSUTCDATETIME()
 );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id=OBJECT_ID('dbo.ecom_tbl_log_dtls') AND name='UX_ecom_log_job_master_run')
 CREATE UNIQUE INDEX UX_ecom_log_job_master_run ON dbo.ecom_tbl_log_dtls(jobid,master_run_id);
GO
