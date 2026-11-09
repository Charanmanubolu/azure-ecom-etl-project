/* Start and finish metadata-driven child pipeline log entries. */
SET XACT_ABORT ON;
GO

CREATE OR ALTER PROCEDURE dbo.ecom_start_log_entry
    @jobid INT,
    @pipeline_id VARCHAR(100),
    @master_pipeline_id VARCHAR(100),
    @master_run_id VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF NULLIF(@master_run_id, '') IS NULL
        THROW 50205, 'ADF master RunId is required.', 1;

    DECLARE @numeric_master_run_id BIGINT;
    SELECT @numeric_master_run_id = master_run_id
    FROM dbo.ecom_adf_run_map
    WHERE adf_master_run_id = @master_run_id;

    IF @numeric_master_run_id IS NULL
        THROW 50206, 'Master run mapping not found. Start the master log first.', 1;

    UPDATE dbo.ecom_tbl_log_dtls
    SET pipeline_id = @pipeline_id,
        master_pipeline_id = @master_pipeline_id,
        job_start_time = SYSUTCDATETIME(),
        job_status = 'Running',
        updated_user = SUSER_SNAME(),
        updated_date = SYSUTCDATETIME()
    WHERE master_run_id = @numeric_master_run_id
      AND jobid = @jobid
      AND job_status = 'Pending';

    IF @@ROWCOUNT <> 1
        THROW 50207, 'Expected exactly one Pending child log row. Check master initialization and duplicate job starts.', 1;

    SELECT log_id
    FROM dbo.ecom_tbl_log_dtls
    WHERE master_run_id = @numeric_master_run_id AND jobid = @jobid;
END;
GO
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
    SET XACT_ABORT ON;

    DECLARE @numeric_master_run_id BIGINT;
    SELECT @numeric_master_run_id = master_run_id
    FROM dbo.ecom_adf_run_map
    WHERE adf_master_run_id = @master_run_id;

    IF @numeric_master_run_id IS NULL
        THROW 50105, 'No numeric master-run mapping for supplied ADF RunId.', 1;

    UPDATE dbo.ecom_tbl_log_dtls
    SET job_end_time = SYSUTCDATETIME(),
        duration_seconds = DATEDIFF_BIG(SECOND, job_start_time, SYSUTCDATETIME()),
        job_status = CASE WHEN @error IS NULL THEN 'Succeeded' ELSE 'Failed' END,
        error_details = @error,
        activity_name = @activity_name,
        error_code = @error_code,
        child_run_id = @child_run_id,
        pipeline_run_id = COALESCE(@child_run_id, pipeline_run_id),
        updated_user = SUSER_SNAME(),
        updated_date = SYSUTCDATETIME()
    WHERE jobid = @jobid
      AND pipeline_id = @pipeline_id
      AND master_run_id = @numeric_master_run_id
      AND job_status = 'Running';

    IF @@ROWCOUNT <> 1
        THROW 50106, 'Expected exactly one running child log row for this job and master run.', 1;
END;
GO
GO
