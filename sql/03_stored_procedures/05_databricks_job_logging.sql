/* Track the Databricks ETL child pipeline separately from source-ingestion jobs. */
SET XACT_ABORT ON;
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
