/*
 Azure E-commerce ETL - Pending status lifecycle
 Apply in sqldb-ecom-dev after 04_master_child_logging.sql.

 Behaviour:
 - Master start creates the master row as Running and registers every configured child as Pending.
 - Child start changes its pre-created row from Pending to Running.
 - Existing child end procedure changes Running to Succeeded/Failed.
 - Master end summarizes completed children; unstarted children remain Pending.
 - No ADF JSON, datasets, or linked services are modified by this SQL script.
*/
SET XACT_ABORT ON;
GO

/* Recreate the status constraint with Pending included. */
DECLARE @dropChecks NVARCHAR(MAX) = N'';
SELECT @dropChecks = @dropChecks +
    N'ALTER TABLE dbo.ecom_tbl_log_dtls DROP CONSTRAINT ' + QUOTENAME(cc.name) + N';'
FROM sys.check_constraints cc
WHERE cc.parent_object_id = OBJECT_ID(N'dbo.ecom_tbl_log_dtls')
  AND cc.definition LIKE N'%job_status%';

IF @dropChecks <> N''
    EXEC sys.sp_executesql @dropChecks;
GO

ALTER TABLE dbo.ecom_tbl_log_dtls
ADD CONSTRAINT CK_ecom_tbl_log_dtls_job_status
CHECK (job_status IN ('Pending','Running','Succeeded','Partial','Failed','Cancelled','Skipped'));
GO

/* Master start: create one master row plus Pending rows for all configured child jobs. */
CREATE OR ALTER PROCEDURE dbo.ecom_start_master_log
    @master_pipeline_id VARCHAR(100),
    @master_run_id VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF NULLIF(@master_pipeline_id, '') IS NULL
        THROW 50201, 'Master pipeline name is required.', 1;
    IF NULLIF(@master_run_id, '') IS NULL
        THROW 50202, 'ADF master RunId is required.', 1;

    DECLARE @numeric_master_run_id BIGINT,
            @master_log_id BIGINT,
            @lock_result INT,
            @lock_resource NVARCHAR(255) = CONCAT('ecom-adf-master-run:', @master_run_id);

    BEGIN TRANSACTION;

    EXEC @lock_result = sys.sp_getapplock
        @Resource = @lock_resource,
        @LockMode = 'Exclusive',
        @LockOwner = 'Transaction',
        @LockTimeout = 15000;

    IF @lock_result < 0
        THROW 50203, 'Could not acquire master-run logging lock.', 1;

    SELECT @numeric_master_run_id = master_run_id
    FROM dbo.ecom_adf_run_map
    WHERE adf_master_run_id = @master_run_id;

    IF @numeric_master_run_id IS NULL
    BEGIN
        SET @numeric_master_run_id = NEXT VALUE FOR dbo.Seq_ecom_master_run_id;
        INSERT dbo.ecom_adf_run_map(adf_master_run_id, master_run_id, master_pipeline_id)
        VALUES(@master_run_id, @numeric_master_run_id, @master_pipeline_id);
    END;

    SELECT @master_log_id = log_id
    FROM dbo.ecom_tbl_log_dtls
    WHERE jobid = 1
      AND master_run_id = @numeric_master_run_id
      AND pipeline_id = @master_pipeline_id;

    IF @master_log_id IS NULL
    BEGIN
        INSERT dbo.ecom_tbl_log_dtls
            (master_run_id, master_pipeline_id, jobid,
             L1_switch_type, L2_switch_type, L3_switch_type, L4_switch_type,
             pipeline_id, pipeline_run_id, job_start_time, job_status,
             created_user, created_date)
        SELECT @numeric_master_run_id, @master_pipeline_id, j.jobid,
               j.L1_switch_type, j.L2_switch_type, j.L3_switch_type, j.L4_switch_type,
               @master_pipeline_id, @master_run_id, SYSUTCDATETIME(), 'Running',
               SUSER_SNAME(), SYSUTCDATETIME()
        FROM dbo.ecom_tbl_job j
        WHERE j.jobid = 1;

        IF @@ROWCOUNT <> 1
            THROW 50204, 'Master job metadata (jobid = 1) was not found.', 1;

        SET @master_log_id = SCOPE_IDENTITY();
    END;

    ;WITH JobConfig AS
    (
        SELECT j.jobid, j.L1_switch_type, j.L2_switch_type, j.L3_switch_type, j.L4_switch_type,
               MAX(CASE WHEN d.dtls_key = 'SourceType' THEN d.dtls_value END) AS SourceType
        FROM dbo.ecom_tbl_job j
        LEFT JOIN dbo.ecom_tbl_job_dtls d ON d.jobid = j.jobid
        WHERE j.jobid <> 1
        GROUP BY j.jobid, j.L1_switch_type, j.L2_switch_type, j.L3_switch_type, j.L4_switch_type
    ),
    ChildJobs AS
    (
        SELECT jobid, L1_switch_type, L2_switch_type, L3_switch_type, L4_switch_type,
               CASE UPPER(SourceType)
                   WHEN 'SQL' THEN 'PL_SQL_TO_RAW'
                   WHEN 'SQLSERVER' THEN 'PL_SQL_TO_RAW'
                   WHEN 'RESTAPI' THEN 'PL_RESTAPI_TO_RAW'
                   WHEN 'CSV' THEN 'PL_CSV_TO_RAW'
                   WHEN 'EXCEL' THEN 'PL_EXCEL_TO_RAW'
                   WHEN 'BULK' THEN 'PL_BULK_TO_RAW'
                   WHEN 'SFTP' THEN 'PL_SFTP_TO_RAW'
                   WHEN 'JSON' THEN 'PL_JSON_TO_RAW'
                   WHEN 'SNOWFLAKE' THEN 'PL_SNOWFLAKE_METADATA_ORCHESTRATOR'
                   WHEN 'TESTFAIL' THEN 'PL_Test_Intentional_Failure'
                   ELSE CONCAT('UNMAPPED_', COALESCE(SourceType, 'NULL'))
               END AS pipeline_id
        FROM JobConfig
    )
    INSERT dbo.ecom_tbl_log_dtls
        (master_run_id, master_pipeline_id, jobid,
         L1_switch_type, L2_switch_type, L3_switch_type, L4_switch_type,
         pipeline_id, pipeline_run_id, job_start_time, job_status,
         created_user, created_date)
    SELECT @numeric_master_run_id, @master_pipeline_id, c.jobid,
           c.L1_switch_type, c.L2_switch_type, c.L3_switch_type, c.L4_switch_type,
           c.pipeline_id, NULL, SYSUTCDATETIME(), 'Pending',
           SUSER_SNAME(), SYSUTCDATETIME()
    FROM ChildJobs c
    WHERE NOT EXISTS
    (
        SELECT 1 FROM dbo.ecom_tbl_log_dtls l
        WHERE l.master_run_id = @numeric_master_run_id AND l.jobid = c.jobid
    );

    COMMIT TRANSACTION;

    SELECT @numeric_master_run_id AS master_run_id, @master_log_id AS log_id;
END;
GO

/* Child start: transition the pre-created Pending row to Running. */
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

/* Master end: keep Pending rows visible when jobs never started. */
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
        THROW 50208, 'No numeric master-run mapping for supplied ADF RunId.', 1;

    SELECT
        @total_child_jobs = COUNT(*),
        @successful_jobs = COALESCE(SUM(CASE WHEN job_status = 'Succeeded' THEN 1 ELSE 0 END), 0),
        @failed_jobs = COALESCE(SUM(CASE WHEN job_status = 'Failed' THEN 1 ELSE 0 END), 0),
        @pending_jobs = COALESCE(SUM(CASE WHEN job_status = 'Pending' THEN 1 ELSE 0 END), 0),
        @running_jobs = COALESCE(SUM(CASE WHEN job_status = 'Running' THEN 1 ELSE 0 END), 0)
    FROM dbo.ecom_tbl_log_dtls
    WHERE master_run_id = @numeric_master_run_id AND jobid <> 1;

    SET @final_status =
        CASE
            WHEN @error IS NOT NULL THEN 'Failed'
            WHEN @pending_jobs = 0 AND @running_jobs = 0
                 AND @total_child_jobs > 0 AND @successful_jobs = @total_child_jobs THEN 'Succeeded'
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
        THROW 50209, 'Expected exactly one Running master log row for this execution.', 1;

    SELECT @numeric_master_run_id AS master_run_id,
           @total_child_jobs AS total_child_jobs,
           @successful_jobs AS successful_jobs,
           @failed_jobs AS failed_jobs,
           @pending_jobs AS pending_jobs,
           @running_jobs AS running_jobs,
           @final_status AS master_status;
END;
GO

/* Verify the status constraint and updated procedure parameters. */
SELECT name, definition
FROM sys.check_constraints
WHERE parent_object_id = OBJECT_ID(N'dbo.ecom_tbl_log_dtls')
  AND definition LIKE N'%job_status%';

SELECT p.name AS ProcedureName, prm.parameter_id, prm.name AS ParameterName,
       TYPE_NAME(prm.user_type_id) AS DataType
FROM sys.procedures p
LEFT JOIN sys.parameters prm ON p.object_id = prm.object_id
WHERE p.name IN ('ecom_start_master_log','ecom_start_log_entry','ecom_end_master_log')
ORDER BY p.name, prm.parameter_id;
GO
