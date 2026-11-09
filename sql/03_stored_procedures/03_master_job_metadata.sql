/* Return source metadata in the shape expected by PL1_Master. */
SET XACT_ABORT ON;
GO

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
GO
