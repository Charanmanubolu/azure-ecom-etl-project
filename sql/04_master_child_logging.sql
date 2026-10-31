/*
 Azure E-commerce ETL - master/child execution logging upgrade
 Apply in sqldb-ecom-dev after 02_logging_upgrade.sql.
 Does not modify ADF pipelines, datasets, or linked services.
*/
SET XACT_ABORT ON;
GO

/* Allow Partial as a master execution outcome. */
DECLARE @dropChecks NVARCHAR(MAX) = N'';
SELECT @dropChecks = @dropChecks +
    N'ALTER TABLE dbo.ecom_tbl_log_dtls DROP CONSTRAINT ' + QUOTENAME(cc.name) + N';'
FROM sys.check_constraints cc
WHERE cc.parent_object_id = OBJECT_ID(N'dbo.ecom_tbl_log_dtls')
  AND cc.definition LIKE N'%job_status%';

IF @dropChecks <> N''
    EXEC sys.sp_executesql @dropChecks;
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.check_constraints
    WHERE parent_object_id = OBJECT_ID(N'dbo.ecom_tbl_log_dtls')
      AND name = N'CK_ecom_tbl_log_dtls_job_status'
)
BEGIN
    ALTER TABLE dbo.ecom_tbl_log_dtls
    ADD CONSTRAINT CK_ecom_tbl_log_dtls_job_status
    CHECK (job_status IN ('Running','Succeeded','Partial','Failed','Cancelled','Skipped'));
END;
GO

/* Create exactly one master log row for each ADF master RunId. */
CREATE OR ALTER PROCEDURE dbo.ecom_start_master_log
    @master_pipeline_id VARCHAR(100),
    @master_run_id VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF NULLIF(@master_pipeline_id, '') IS NULL
        THROW 50101, 'Master pipeline name is required.', 1;
    IF NULLIF(@master_run_id, '') IS NULL
        THROW 50102, 'ADF master RunId is required.', 1;

    DECLARE @numeric_master_run_id BIGINT,
            @log_id BIGINT,
            @lock_result INT,
            @lock_resource NVARCHAR(255) = CONCAT('ecom-adf-master-run:', @master_run_id);

    BEGIN TRANSACTION;

    EXEC @lock_result = sys.sp_getapplock
        @Resource = @lock_resource,
        @LockMode = 'Exclusive',
        @LockOwner = 'Transaction',
        @LockTimeout = 15000;

    IF @lock_result < 0
        THROW 50103, 'Could not acquire master-run logging lock.', 1;

    SELECT @numeric_master_run_id = master_run_id
    FROM dbo.ecom_adf_run_map
    WHERE adf_master_run_id = @master_run_id;

    IF @numeric_master_run_id IS NULL
    BEGIN
        SET @numeric_master_run_id = NEXT VALUE FOR dbo.Seq_ecom_master_run_id;

        INSERT dbo.ecom_adf_run_map
            (adf_master_run_id, master_run_id, master_pipeline_id)
        VALUES
            (@master_run_id, @numeric_master_run_id, @master_pipeline_id);
    END;

    SELECT @log_id = log_id
    FROM dbo.ecom_tbl_log_dtls
    WHERE jobid = 1
      AND master_run_id = @numeric_master_run_id
      AND pipeline_id = @master_pipeline_id;

    IF @log_id IS NULL
    BEGIN
        INSERT dbo.ecom_tbl_log_dtls
            (master_run_id, master_pipeline_id, jobid,
             L1_switch_type, L2_switch_type, L3_switch_type, L4_switch_type,
             pipeline_id, pipeline_run_id, job_start_time, job_status,
             created_user, created_date)
        SELECT
            @numeric_master_run_id, @master_pipeline_id, j.jobid,
            j.L1_switch_type, j.L2_switch_type, j.L3_switch_type, j.L4_switch_type,
            @master_pipeline_id, @master_run_id, SYSUTCDATETIME(), 'Running',
            SUSER_SNAME(), SYSUTCDATETIME()
        FROM dbo.ecom_tbl_job j
        WHERE j.jobid = 1;

        IF @@ROWCOUNT <> 1
            THROW 50104, 'Master job metadata (jobid = 1) was not found.', 1;

        SET @log_id = SCOPE_IDENTITY();
    END;

    COMMIT TRANSACTION;

    SELECT @numeric_master_run_id AS master_run_id, @log_id AS log_id;
END;
GO

/* Finalize a child row and retain its actual ADF child pipeline RunId. */
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

/* Finalize the master row after all child jobs have completed. */
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
            @running_jobs INT,
            @expected_child_jobs INT,
            @final_status VARCHAR(20);

    SELECT @numeric_master_run_id = master_run_id
    FROM dbo.ecom_adf_run_map
    WHERE adf_master_run_id = @master_run_id;

    IF @numeric_master_run_id IS NULL
        THROW 50107, 'No numeric master-run mapping for supplied ADF RunId.', 1;

    SELECT
        @total_child_jobs = COUNT(*),
        @successful_jobs = COALESCE(SUM(CASE WHEN job_status = 'Succeeded' THEN 1 ELSE 0 END), 0),
        @failed_jobs = COALESCE(SUM(CASE WHEN job_status = 'Failed' THEN 1 ELSE 0 END), 0),
        @running_jobs = COALESCE(SUM(CASE WHEN job_status = 'Running' THEN 1 ELSE 0 END), 0)
    FROM dbo.ecom_tbl_log_dtls
    WHERE master_run_id = @numeric_master_run_id
      AND jobid <> 1;

    SELECT @expected_child_jobs = COUNT(*)
    FROM dbo.ecom_tbl_job
    WHERE jobid <> 1;

    SET @final_status =
        CASE
            WHEN @error IS NOT NULL THEN 'Failed'
            WHEN @running_jobs > 0 THEN 'Failed'
            WHEN @total_child_jobs = @expected_child_jobs
                 AND @successful_jobs = @expected_child_jobs THEN 'Succeeded'
            WHEN @successful_jobs > 0 THEN 'Partial'
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
        THROW 50108, 'Expected exactly one running master log row for this execution.', 1;

    SELECT
        @numeric_master_run_id AS master_run_id,
        @total_child_jobs AS total_child_jobs,
        @successful_jobs AS successful_jobs,
        @failed_jobs AS failed_jobs,
        @final_status AS master_status;
END;
GO

/* Verify the new procedure signatures and supported statuses. */
SELECT p.name AS ProcedureName, prm.parameter_id, prm.name AS ParameterName,
       TYPE_NAME(prm.user_type_id) AS DataType
FROM sys.procedures p
LEFT JOIN sys.parameters prm ON p.object_id = prm.object_id
WHERE p.name IN ('ecom_start_master_log','ecom_end_master_log','ecom_end_log_entry')
ORDER BY p.name, prm.parameter_id;

SELECT name, definition
FROM sys.check_constraints
WHERE parent_object_id = OBJECT_ID(N'dbo.ecom_tbl_log_dtls')
  AND definition LIKE N'%job_status%';
GO
