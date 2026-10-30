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

CREATE OR ALTER PROCEDURE dbo.ecom_start_log_entry
 @jobid INT,
 @pipeline_id VARCHAR(100),
 @master_pipeline_id VARCHAR(100),
 @master_run_id VARCHAR(100)
AS
BEGIN
 SET NOCOUNT ON;
 SET XACT_ABORT ON;
 IF NULLIF(@master_run_id,'') IS NULL THROW 50011,'ADF master RunId is required.',1;

 DECLARE @numeric_master_run_id BIGINT, @lock_result INT;
 DECLARE @lock_resource NVARCHAR(255)=CONCAT('ecom-adf-master-run:',@master_run_id);
 BEGIN TRANSACTION;
 EXEC @lock_result=sys.sp_getapplock
  @Resource=@lock_resource,
  @LockMode='Exclusive',@LockOwner='Transaction',@LockTimeout=15000;
 IF @lock_result<0 THROW 50013,'Could not acquire master-run logging lock.',1;

 SELECT @numeric_master_run_id=master_run_id
 FROM dbo.ecom_adf_run_map WHERE adf_master_run_id=@master_run_id;

 IF @numeric_master_run_id IS NULL
 BEGIN
  SET @numeric_master_run_id=NEXT VALUE FOR dbo.Seq_ecom_master_run_id;
  INSERT dbo.ecom_adf_run_map(adf_master_run_id,master_run_id,master_pipeline_id)
  VALUES(@master_run_id,@numeric_master_run_id,@master_pipeline_id);
 END;

 IF EXISTS(SELECT 1 FROM dbo.ecom_tbl_log_dtls WHERE jobid=@jobid AND master_run_id=@numeric_master_run_id)
  THROW 50014,'A log entry already exists for this job and master run.',1;

 INSERT dbo.ecom_tbl_log_dtls
 (master_run_id,master_pipeline_id,jobid,L1_switch_type,L2_switch_type,L3_switch_type,L4_switch_type,
  pipeline_id,job_start_time,job_status,created_user,created_date)
 SELECT @numeric_master_run_id,@master_pipeline_id,j.jobid,j.L1_switch_type,j.L2_switch_type,
  j.L3_switch_type,j.L4_switch_type,@pipeline_id,SYSUTCDATETIME(),'Running',SUSER_SNAME(),SYSUTCDATETIME()
 FROM dbo.ecom_tbl_job j WHERE j.jobid=@jobid;

 IF @@ROWCOUNT<>1 THROW 50015,'Job metadata not found for supplied jobid.',1;
 COMMIT TRANSACTION;
 SELECT CONVERT(BIGINT,SCOPE_IDENTITY()) AS log_id;
END;
GO

CREATE OR ALTER PROCEDURE dbo.ecom_end_log_entry
 @jobid INT,
 @pipeline_id VARCHAR(100),
 @master_run_id VARCHAR(100),
 @error VARCHAR(MAX)=NULL,
 @activity_name VARCHAR(200)=NULL,
 @error_code VARCHAR(100)=NULL,
 @child_run_id VARCHAR(100)=NULL
AS
BEGIN
 SET NOCOUNT ON;
 SET XACT_ABORT ON;
 DECLARE @numeric_master_run_id BIGINT;
 SELECT @numeric_master_run_id=master_run_id
 FROM dbo.ecom_adf_run_map WHERE adf_master_run_id=@master_run_id;
 IF @numeric_master_run_id IS NULL
  THROW 50016,'No numeric master-run mapping for supplied ADF RunId.',1;

 UPDATE dbo.ecom_tbl_log_dtls
 SET job_end_time=SYSUTCDATETIME(),
     duration_seconds=DATEDIFF_BIG(SECOND,job_start_time,SYSUTCDATETIME()),
     job_status=CASE WHEN @error IS NULL THEN 'Succeeded' ELSE 'Failed' END,
     error_details=@error,activity_name=@activity_name,error_code=@error_code,
     child_run_id=@child_run_id,updated_user=SUSER_SNAME(),updated_date=SYSUTCDATETIME()
 WHERE jobid=@jobid AND pipeline_id=@pipeline_id
   AND master_run_id=@numeric_master_run_id AND job_status='Running';

 IF @@ROWCOUNT<>1
  THROW 50012,'Expected exactly one running log row for this job and master run.',1;
END;
GO

/* The current PL1_Master Switch routes on 'SQL', while metadata stores 'SQLServer'.
   Normalize only the value returned to ADF; stored configuration is not modified. */
CREATE OR ALTER PROCEDURE dbo.ecom_get_master_job_metadata
AS
BEGIN
 SET NOCOUNT ON;
 SELECT t.trigger_id,t.trigger_name,j.jobid,j.L1_switch_type,j.L2_switch_type,j.L3_switch_type,j.L4_switch_type,
  CASE WHEN MAX(CASE WHEN d.dtls_key='SourceType' THEN d.dtls_value END)='SQLServer'
       THEN 'SQL' ELSE MAX(CASE WHEN d.dtls_key='SourceType' THEN d.dtls_value END) END AS SourceType,
  MAX(CASE WHEN d.dtls_key='SourceContainer' THEN d.dtls_value END) AS SourceContainer,
  MAX(CASE WHEN d.dtls_key='SourceDirectory' THEN d.dtls_value END) AS SourceDirectory,
  MAX(CASE WHEN d.dtls_key='SourceName' THEN d.dtls_value END) AS SourceName,
  MAX(CASE WHEN d.dtls_key='SourceDatabase' THEN d.dtls_value END) AS SourceDatabase,
  MAX(CASE WHEN d.dtls_key='SourceSchema' THEN d.dtls_value END) AS SourceSchema,
  MAX(CASE WHEN d.dtls_key='SourceTable' THEN d.dtls_value END) AS SourceTable,
  MAX(CASE WHEN d.dtls_key='SourceObject' THEN d.dtls_value END) AS SourceObject,
  MAX(CASE WHEN d.dtls_key='WatermarkColumn' THEN d.dtls_value END) AS WatermarkColumn,
  MAX(CASE WHEN d.dtls_key='LoadType' THEN d.dtls_value END) AS LoadType,
  MAX(CASE WHEN d.dtls_key='SourceQuery' THEN d.dtls_value END) AS SourceQuery,
  MAX(CASE WHEN d.dtls_key='BaseURL' THEN d.dtls_value END) AS BaseURL,
  MAX(CASE WHEN d.dtls_key='RelativeURL' THEN d.dtls_value END) AS RelativeURL,
  MAX(CASE WHEN d.dtls_key='HTTPMethod' THEN d.dtls_value END) AS HTTPMethod,
  MAX(CASE WHEN d.dtls_key='TargetContainer' THEN d.dtls_value END) AS TargetContainer,
  MAX(CASE WHEN d.dtls_key='TargetEntity' THEN d.dtls_value END) AS TargetEntity,
  MAX(CASE WHEN d.dtls_key='TargetFormat' THEN d.dtls_value END) AS TargetFormat
 FROM dbo.ecom_tbl_trigger t
 JOIN dbo.ecom_tbl_job j ON j.trigger_id=t.trigger_id
 JOIN dbo.ecom_tbl_job_dtls d ON d.jobid=j.jobid
 WHERE t.trigger_name<>'TR_ECOM_MASTER'
 GROUP BY t.trigger_id,t.trigger_name,j.jobid,j.L1_switch_type,j.L2_switch_type,j.L3_switch_type,j.L4_switch_type
 ORDER BY j.jobid;
END;
GO

SELECT name FROM sys.procedures
WHERE name IN('ecom_start_log_entry','ecom_end_log_entry','ecom_get_master_job_metadata')
ORDER BY name;
GO
