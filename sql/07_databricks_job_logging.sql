/*
 Azure E-commerce ETL - Databricks job execution logging.
 Run once in sqldb-ecom-dev before publishing/running PL1_Master.
*/
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

CREATE OR ALTER PROCEDURE dbo.ecom_start_databricks_log
    @master_pipeline_id VARCHAR(100),
    @master_run_id VARCHAR(100),
    @child_pipeline_id VARCHAR(100),
    @job_id BIGINT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF NULLIF(@master_run_id, '') IS NULL
        THROW 50401, 'ADF master RunId is required.', 1;

    IF EXISTS (
        SELECT 1 FROM dbo.ecom_databricks_job_log
        WHERE master_run_id = @master_run_id AND child_pipeline_id = @child_pipeline_id
    )
        THROW 50402, 'Databricks execution log already exists for this master run.', 1;

    INSERT dbo.ecom_databricks_job_log
        (master_pipeline_id, master_run_id, child_pipeline_id, job_id, job_status, job_start_time)
    VALUES
        (@master_pipeline_id, @master_run_id, @child_pipeline_id, @job_id, 'Running', SYSUTCDATETIME());
END;
GO

CREATE OR ALTER PROCEDURE dbo.ecom_end_databricks_log
    @master_run_id VARCHAR(100),
    @child_pipeline_id VARCHAR(100),
    @child_pipeline_run_id VARCHAR(100) = NULL,
    @error VARCHAR(MAX) = NULL,
    @activity_name VARCHAR(200) = NULL,
    @error_code VARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    UPDATE dbo.ecom_databricks_job_log
    SET child_pipeline_run_id = @child_pipeline_run_id,
        job_end_time = SYSUTCDATETIME(),
        duration_seconds = DATEDIFF_BIG(SECOND, job_start_time, SYSUTCDATETIME()),
        job_status = CASE WHEN @error IS NULL THEN 'Succeeded' ELSE 'Failed' END,
        error_details = @error,
        activity_name = @activity_name,
        error_code = @error_code
    WHERE master_run_id = @master_run_id
      AND child_pipeline_id = @child_pipeline_id
      AND job_status = 'Running';

    IF @@ROWCOUNT <> 1
        THROW 50403, 'Expected exactly one running Databricks execution log.', 1;
END;
GO
