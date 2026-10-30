/*
 Azure E-commerce ETL - watermark procedure used by the existing ADF pipelines.
 ADF activities are unchanged. This procedure accepts the six parameters
 already configured in the SQL, CSV, Excel, Bulk, JSON, SFTP and Snowflake pipelines.
 Existing watermark rows are updated in place; missing rows are inserted.
*/
CREATE OR ALTER PROCEDURE dbo.ecom_update_watermark
 @source_type VARCHAR(50),
 @source_name VARCHAR(250),
 @schema_name VARCHAR(128),
 @table_name VARCHAR(250),
 @watermark_column VARCHAR(128),
 @last_load_value VARCHAR(200)
AS
BEGIN
 SET NOCOUNT ON;
 SET XACT_ABORT ON;

 DECLARE @source_object VARCHAR(250),
         @resolved_table_name VARCHAR(250),
         @resolved_source_name VARCHAR(250);

 SET @resolved_table_name=@table_name;
 SET @resolved_source_name=@source_name;

 /* SourceTable is the short logical name used by ADF (orders);
    SourceObject is the physical dataset name stored in metadata. */
 SELECT TOP (1) @source_object=source_object.dtls_value
 FROM dbo.ecom_tbl_job_dtls source_table
 JOIN dbo.ecom_tbl_job_dtls source_type
   ON source_type.jobid=source_table.jobid
  AND source_type.dtls_key='SourceType'
 JOIN dbo.ecom_tbl_job_dtls source_object
   ON source_object.jobid=source_table.jobid
  AND source_object.dtls_key='SourceObject'
 WHERE source_table.dtls_key='SourceTable'
   AND source_table.dtls_value=@table_name
   AND source_type.dtls_value=@source_type;

 IF NULLIF(@source_object,'') IS NOT NULL
    SET @resolved_table_name=@source_object;

 /* Preserve the already registered source name/table key when an existing
    watermark row matches this logical or physical table. */
 SELECT TOP (1)
    @resolved_source_name=source_name,
    @resolved_table_name=table_name
 FROM dbo.ecom_tbl_watermark
 WHERE source_type=@source_type
   AND schema_name=@schema_name
   AND watermark_column=@watermark_column
   AND table_name IN (@table_name,@resolved_table_name)
 ORDER BY CASE WHEN table_name=@resolved_table_name THEN 0 ELSE 1 END;

 IF EXISTS
 (
   SELECT 1 FROM dbo.ecom_tbl_watermark
   WHERE source_type=@source_type
     AND source_name=@resolved_source_name
     AND schema_name=@schema_name
     AND table_name=@resolved_table_name
 )
 BEGIN
   UPDATE dbo.ecom_tbl_watermark
   SET watermark_column=@watermark_column,
       last_load_value=@last_load_value,
       last_successful_load_time=SYSUTCDATETIME(),
       updated_date=SYSUTCDATETIME()
   WHERE source_type=@source_type
     AND source_name=@resolved_source_name
     AND schema_name=@schema_name
     AND table_name=@resolved_table_name;
 END
 ELSE
 BEGIN
   INSERT INTO dbo.ecom_tbl_watermark
   (
     source_type,source_name,schema_name,table_name,
     watermark_column,last_load_value,last_successful_load_time,
     created_date,updated_date
   )
   VALUES
   (
     @source_type,@resolved_source_name,@schema_name,@resolved_table_name,
     @watermark_column,@last_load_value,SYSUTCDATETIME(),
     SYSUTCDATETIME(),SYSUTCDATETIME()
   );
 END;
END;
GO
