declare @Version decimal(5, 3)  = (select cast(left(cast(serverproperty('productversion') as varchar), 4) as decimal(5, 3)))

IF @Version < 10.51
Begin

	declare @Drives table  (Letter varchar(3), SpaceFree bigint)

	insert into @Drives
	EXEC MASTER..xp_fixeddrives

	Select 
	Letter,
	SpaceFree/1024  as "GB Free"
	from @Drives

End
Else

SELECT DISTINCT
  vs.volume_mount_point AS [Drive],
  vs.logical_volume_name AS [Drive Name],
  vs.total_bytes/1024/1024/1024 AS [Drive Size GB],
  vs.available_bytes/1024/1024/1024 AS [Drive Free Space GB],
  cast(100 - ((((vs.available_bytes/1024/1024/1024) * 100.0)/ (vs.total_bytes/1024/1024/1024)))as decimal(5,2)) AS [% FULL]

FROM sys.master_files AS f
CROSS APPLY sys.dm_os_volume_stats(f.database_id, f.file_id) AS vs
ORDER BY vs.volume_mount_point;

--sp_who3
--sp_whoisactive

-- sp_who2 53

--select @@version
