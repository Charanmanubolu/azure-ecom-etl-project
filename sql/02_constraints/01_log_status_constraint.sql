/* Supported lifecycle states for child and master ADF log rows. */
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
