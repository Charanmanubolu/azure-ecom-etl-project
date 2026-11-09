/* Dedicated execution history for the Databricks ETL child pipeline. */
SET XACT_ABORT ON;
GO

IF OBJECT_ID(N'dbo.ecom_databricks_job_log', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.ecom_databricks_job_log
    (
        log_id BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_ecom_databricks_job_log PRIMARY KEY,
        master_pipeline_id VARCHAR(100) NOT NULL,
        master_run_id VARCHAR(100) NOT NULL,
        child_pipeline_id VARCHAR(100) NOT NULL,
        child_pipeline_run_id VARCHAR(100) NULL,
        job_id BIGINT NOT NULL,
        job_status VARCHAR(20) NOT NULL,
        job_start_time DATETIME2(7) NOT NULL,
        job_end_time DATETIME2(7) NULL,
        duration_seconds BIGINT NULL,
        error_details VARCHAR(MAX) NULL,
        activity_name VARCHAR(200) NULL,
        error_code VARCHAR(100) NULL,
        created_date DATETIME2(7) NOT NULL CONSTRAINT DF_ecom_databricks_job_log_created DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_ecom_databricks_job_log_run UNIQUE(master_run_id, child_pipeline_id),
        CONSTRAINT CK_ecom_databricks_job_log_status CHECK (job_status IN ('Running','Succeeded','Failed'))
    );
END;
GO
