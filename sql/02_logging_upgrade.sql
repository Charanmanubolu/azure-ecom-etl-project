/*
  Azure E-commerce ETL - execution-safe pipeline logging upgrade
  Non-destructive migration: preserves all existing log rows.
  Deploy this script to sqldb-ecom-dev before publishing the updated ADF pipeline.
*/
SET XACT_ABORT ON;
BEGIN TRY
    BEGIN TRANSACTION;

    IF COL_LENGTH('dbo.ecom_tbl_log_dtls','master_pipeline_id') IS NULL
        ALTER TABLE dbo.ecom_tbl_log_dtls ADD master_pipeline_id VARCHAR(100) NULL;
    IF COL_LENGTH('dbo.ecom_tbl_log_dtls','master_run_id') IS NULL
        ALTER TABLE dbo.ecom_tbl_log_dtls ADD master_run_id VARCHAR(100) NULL;
    IF COL_LENGTH('dbo.ecom_tbl_log_dtls','child_run_id') IS NULL
        ALTER TABLE dbo.ecom_tbl_log_dtls ADD child_run_id VARCHAR(100) NULL;
    IF COL_LENGTH('dbo.ecom_tbl_log_dtls','activity_name') IS NULL
        ALTER TABLE dbo.ecom_tbl_log_dtls ADD activity_name VARCHAR(200) NULL;
    IF COL_LENGTH('dbo.ecom_tbl_log_dtls','error_code') IS NULL
        ALTER TABLE dbo.ecom_tbl_log_dtls ADD error_code VARCHAR(100) NULL;

    IF NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE object_id = OBJECT_ID('dbo.ecom_tbl_log_dtls')
          AND name = 'UX_ecom_log_job_master_run'
    )
        CREATE UNIQUE INDEX UX_ecom_log_job_master_run
        ON dbo.ecom_tbl_log_dtls(jobid, master_run_id)
        WHERE master_run_id IS NOT NULL;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
    THROW;
END CATCH;
GO

CREATE OR ALTER PROCEDURE dbo.ecom_start_log_entry
    @jobid INT,
    @pipeline_id VARCHAR(100),
    @master_pipeline_id VARCHAR(100),
    @master_run_id VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (
        SELECT 1 FROM dbo.ecom_tbl_log_dtls
        WHERE jobid = @jobid AND master_run_id = @master_run_id
    )
        THROW 50011, 'A log entry already exists for this job and master run.', 1;

    INSERT INTO dbo.ecom_tbl_log_dtls
    (
        jobid, pipeline_id, master_pipeline_id, master_run_id,
        job_start_time, job_status, created_user, created_date
    )
    VALUES
    (
        @jobid, @pipeline_id, @master_pipeline_id, @master_run_id,
        SYSUTCDATETIME(), 'Running', SUSER_SNAME(), SYSUTCDATETIME()
    );

    SELECT CONVERT(BIGINT, SCOPE_IDENTITY()) AS log_id;
END;
GO

CREATE OR ALTER PROCEDURE dbo.ecom_end_log_entry
    @jobid INT,
    @pipeline_id VARCHAR(100),
    @master_run_id VARCHAR(100),
    @error VARCHAR(MAX) = NULL,
    @activity_name VARCHAR(200) = NULL,
    @error_code VARCHAR(100) = NULL,
    @child_run_id VARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE dbo.ecom_tbl_log_dtls
    SET
        job_end_time = SYSUTCDATETIME(),
        job_status = CASE WHEN @error IS NULL THEN 'Completed' ELSE 'Failed' END,
        error_dtls = @error,
        activity_name = @activity_name,
        error_code = @error_code,
        child_run_id = @child_run_id,
        updated_user = SUSER_SNAME(),
        updated_date = SYSUTCDATETIME()
    WHERE jobid = @jobid
      AND pipeline_id = @pipeline_id
      AND master_run_id = @master_run_id
      AND job_status = 'Running';

    IF @@ROWCOUNT <> 1
        THROW 50012, 'Expected exactly one running log row for this job and master run.', 1;
END;
GO

/* Post-deployment verification */
SELECT TOP (20)
    log_id, jobid, pipeline_id, master_pipeline_id, master_run_id,
    child_run_id, job_status, activity_name, error_code, error_dtls,
    job_start_time, job_end_time
FROM dbo.ecom_tbl_log_dtls
ORDER BY log_id DESC;
