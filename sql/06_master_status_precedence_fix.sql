/*
 Fix master status precedence:
 - Succeeded only when every configured child succeeded and no master error exists.
 - Partial when at least one child succeeded but another failed, remained pending/running,
   or the orchestration reported an error.
 - Failed when no child succeeded.
 Apply in sqldb-ecom-dev after 05_pending_status_lifecycle.sql.
*/
CREATE OR ALTER PROCEDURE dbo.ecom_end_master_log
    @master_pipeline_id VARCHAR(100),
    @master_run_id VARCHAR(100),
    @error VARCHAR(MAX) = NULL,
    @activity_name VARCHAR(200) = NULL,
    @error_code VARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @numeric_master_run_id BIGINT,
            @total_child_jobs INT,
            @successful_jobs INT,
            @failed_jobs INT,
            @pending_jobs INT,
            @running_jobs INT,
            @final_status VARCHAR(20);

    SELECT @numeric_master_run_id = master_run_id
    FROM dbo.ecom_adf_run_map
    WHERE adf_master_run_id = @master_run_id;

    IF @numeric_master_run_id IS NULL
        THROW 50301, 'No numeric master-run mapping for supplied ADF RunId.', 1;

    SELECT
        @total_child_jobs = COUNT(*),
        @successful_jobs = COALESCE(SUM(CASE WHEN job_status = 'Succeeded' THEN 1 ELSE 0 END), 0),
        @failed_jobs = COALESCE(SUM(CASE WHEN job_status = 'Failed' THEN 1 ELSE 0 END), 0),
        @pending_jobs = COALESCE(SUM(CASE WHEN job_status = 'Pending' THEN 1 ELSE 0 END), 0),
        @running_jobs = COALESCE(SUM(CASE WHEN job_status = 'Running' THEN 1 ELSE 0 END), 0)
    FROM dbo.ecom_tbl_log_dtls
    WHERE master_run_id = @numeric_master_run_id
      AND jobid <> 1;

    SET @final_status =
        CASE
            WHEN @successful_jobs > 0
                 AND (
                     @error IS NOT NULL
                     OR @failed_jobs > 0
                     OR @pending_jobs > 0
                     OR @running_jobs > 0
                     OR @successful_jobs < @total_child_jobs
                 )
                THEN 'Partial'
            WHEN @total_child_jobs > 0
                 AND @successful_jobs = @total_child_jobs
                 AND @error IS NULL
                 AND @pending_jobs = 0
                 AND @running_jobs = 0
                THEN 'Succeeded'
            ELSE 'Failed'
        END;

    UPDATE dbo.ecom_tbl_log_dtls
    SET job_end_time = SYSUTCDATETIME(),
        duration_seconds = DATEDIFF_BIG(SECOND, job_start_time, SYSUTCDATETIME()),
        job_status = @final_status,
        error_details = @error,
        activity_name = @activity_name,
        error_code = @error_code,
        total_child_jobs = @total_child_jobs,
        successful_jobs = @successful_jobs,
        failed_jobs = @failed_jobs,
        updated_user = SUSER_SNAME(),
        updated_date = SYSUTCDATETIME()
    WHERE jobid = 1
      AND pipeline_id = @master_pipeline_id
      AND master_run_id = @numeric_master_run_id
      AND job_status = 'Running';

    IF @@ROWCOUNT <> 1
        THROW 50302, 'Expected exactly one Running master log row for this execution.', 1;

    SELECT @numeric_master_run_id AS master_run_id,
           @total_child_jobs AS total_child_jobs,
           @successful_jobs AS successful_jobs,
           @failed_jobs AS failed_jobs,
           @pending_jobs AS pending_jobs,
           @running_jobs AS running_jobs,
           @final_status AS master_status;
END;
GO
