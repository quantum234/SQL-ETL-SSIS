
SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO


CREATE proc [dbo].[MNT_MaintenanceIndexes]
As
--==========================================
/*
Process220MaintenanceIndexes -- Rebuild Indexes
V02_20110601 - EReiman -- revised version of 220 which only scans fragmentation based on a definable schedule (does not rescan if data exists within X days)
	-- stores the index stats in a table, and then rebuilds based on this data, and a definable schedule for rebuilds
	-- configurable via input params as well as a config table, allowing specific settings for each index
	-- also configure rebuild settings such as compression and MAXDOP
	-- rebuilds by partition
	-- handles LOBs
	-- conditionally inserts into CardlyticsMessageLog (if the table exists)
	-- stores history in dbo.IndexRebuildHist
	-- looks for unprocessed indexes which were scanned within @daysOfHistoryToProcess days
	-- gracefully exits after @maxRuntimeSeconds seconds 
V04_20110603 - EReiman -- added logging into CML
V05_20110725 - EReiman -- Changed default for @default_DATA_COMPRESSION from NULL to NONE
EXAMPLE:
exec Process220_TESTING @default_DaysToRescan = 7, @maxRuntimeSeconds = 300, @daysOfHistoryToProcess = 1, @default_FragPercent = 25

20110912 - TA1821 - JLK - Changed default compression to 'PAGE', Fill Factor to 90, implemented new LogIt logger, added new timeout config setting for weekdays and weekends, ability to only run on weekend,
					Ability to force rescanning of indexes on the weekend. Supporting tables moved to cardutil schema. Input params moved to standard params. New param to log history table details.
					Config values starting with process220 are no longer used.
					New Config Values : 
					MNT_MaintenanceIndexes_DaysToRescan,MNT_MaintenanceIndexes_FillFactor,MNT_MaintenanceIndexes_FragmentationLevel,MNT_MaintenanceIndexes_LastRunDate,
					MNT_MaintenanceIndexes_LogIndexDetails,MNT_MaintenanceIndexes_MaxRunTime_Weekday,MNT_MaintenanceIndexes_MaxRunTime_Weekend,
					MNT_MaintenanceIndexes_OnlyRunOnWeekends,MNT_MaintenanceIndexes_RescanOnWeekend
					* We are currently not using the IndexRebuildConfig table.
20111026- DE1052 Migrated fixes from 2.X. in regards to DaysToRescan.		
20120117- DE1376 - JLK - Modified to handle and log when a prior scanned index does not exist.
20121022- TA9643 - JMR - Created two new options/config settings (Max DOP and FragPercent Level to Rebuild) and reassigned the existing FragPercent config setting to indicate when to Reorganize.
                          The indexes will be either rebuilt or reorganized based on the fragmentation percentage. The reorganize will only occur if AllowPageLocks is True and index is not disabled.
                          REORGANIZE: This will be performed only when the index has AllowPageLocks=True and the index is still enabled. 
									  As well, the fragmentation level MUST BE GREATER THAN/EQUAL TO the level specified in config setting for "FragLevel_Reorganize" AND LESS THAN "FragLevel_Rebuild".
						  REBUILD: This will be performed only when the fragmentation level is GREATER THAN/EQUAL TO the level specified in config setting "FragLevel_Rebuild".
						  The values in the cardutil.IndexRebuildConfig table take priority over the configuration settings in the event the index is listed in the table.
20130116- 12730 - JLK Modify Index Maintenance SP to not have lookback of history to process hardcoded to 3. It will now match the daysToRescan config value.
20130117- 12730 - JLK When rebuilding, choose the most recent rebuild statement
20130201 - 13774 - JMR - Add deadlock/retry logic to improve resiliency.
20130305 - 16823 - JLK - Correct choosing index when there are multiple partitions.
20130409 JLK 17005. Add NoLocks to system views / tables where needed in OPSBatch
*/
--==========================================

BEGIN
SET NOCOUNT ON
DECLARE @MaintenanceIndexesOnlyRunOnWeekendSetting INT = 0 -- Default to everyday
		, @StartDT DATETIME = GETDATE()
		, @eMessage VARCHAR(8000) = '' 
		, @ExitType INT = 0
		, @LastRunDate DATETIME
		, @maxRuntimeSeconds INT
		, @default_DaysToRescan SMALLINT = 7 -- This can change on the weekend.
		, @default_FragPercentReorganize tinyint = 30
		, @default_FragPercentRebuild tinyint = 90
		, @default_PAD_INDEX varchar(32) = 'ON'
		, @default_STATISTICS_NORECOMPUTE varchar(32) = 'OFF'
		, @default_SORT_IN_TEMPDB varchar(32) = 'OFF'
		, @default_IGNORE_DUP_KEY varchar(32) = 'OFF'
		, @default_ONLINE varchar(32) = 'ON'
		, @default_ALLOW_ROW_LOCKS varchar(32) = 'ON'
		, @default_ALLOW_PAGE_LOCKS varchar(32) = 'ON'
		, @default_FILLFACTOR varchar(32) = '90'
		, @default_DATA_COMPRESSION varchar(32) = 'PAGE' -- options = 'NONE','ROW','PAGE'.  If null, then does not include that clause in the SQL statement -- thus effectively defaulting to 'NONE'
		, @default_MAXDOP SMALLINT = 0
		, @daysOfHistoryToProcess int = 15 -- # of days to look back for unprocessed indexes to process.  These would be null related to a forced timeout from @maxRuntimeSeconds where the frag% was scanned, but the index not rebuilt
		, @outputSQL bit = 0
		, @executeSQL bit = 1
		, @insertIntoCML bit = 1
		, @performRebuild bit = 1 -- if 0 then only scans fragmentation -- does not rebuild
		, @cStartDt DATETIME = GETDATE()
		, @cMessage VARCHAR(8000)
		, @LogHistoryDetails BIT = 0		
		, @RescanOnWeekend BIT = 1
		, @msg VARCHAR(4000) = ''
		, @DeadLockTotalTries INT
		, @DeadLockDelay VARCHAR(30)
		, @DeadlockTries INT = 0

SELECT @MaintenanceIndexesOnlyRunOnWeekendSetting	= COALESCE((select cast(value as int) from dbo.Configuration WITH (NOLOCK) where configurationID = 'MNT_MaintenanceIndexes_OnlyRunOnWeekends'), 1), 
		@LastRunDate = COALESCE((select cast(value as datetime) from dbo.Configuration WITH (NOLOCK) where configurationID = 'MNT_MaintenanceIndexes_LastRunDate'), GETDATE() - 10),
		@maxRuntimeSeconds = COALESCE((select cast(value as int) from dbo.Configuration WITH (NOLOCK) where configurationID = 'MNT_MaintenanceIndexes_MaxRunTime_Weekday'), 3600),
		@default_FragPercentReorganize = COALESCE((select cast(value as TINYINT) from dbo.Configuration WITH (NOLOCK) where configurationID = 'MNT_MaintenanceIndexes_FragLevel_Reorganize'), 30),
		@default_FragPercentRebuild = COALESCE((select cast(value as TINYINT) from dbo.Configuration WITH (NOLOCK) where configurationID = 'MNT_MaintenanceIndexes_FragLevel_Rebuild'), 90),
		@default_FILLFACTOR = COALESCE((select cast(value as varchar(32)) from dbo.Configuration WITH (NOLOCK) where configurationID = 'MNT_MaintenanceIndexes_FillFactor'), 90), 
		@LogHistoryDetails = COALESCE((select cast(value as bit) from dbo.Configuration WITH (NOLOCK) where configurationID = 'MNT_MaintenanceIndexes_LogIndexDetails'), 0),
		@default_DaysToRescan = COALESCE((select cast(value as SMALLINT) from dbo.Configuration WITH (NOLOCK) where configurationID = 'MNT_MaintenanceIndexes_DaysToRescan'), 3),
		@RescanOnWeekend = COALESCE((select cast(value as BIT) from dbo.Configuration WITH (NOLOCK) where configurationID = 'MNT_MaintenanceIndexes_RescanOnWeekend'), 1),
		@default_MAXDOP = COALESCE((select cast(value as SMALLINT) from dbo.Configuration WITH (NOLOCK) where configurationID = 'MNT_MaintenanceIndexes_MaxDegreeParallelism'), 0),
		@DeadLockTotalTries = COALESCE((SELECT CAST(Value AS INT) FROM dbo.Configuration WITH (NOLOCK) WHERE ConfigurationID = 'Deadlock_Retries_Total_Times_To_Retry'), 60), 
		@DeadLockDelay = COALESCE((SELECT CAST(Value AS VARCHAR(30)) FROM dbo.Configuration WITH (NOLOCK) WHERE ConfigurationID = 'Deadlock_Retries_Delay_Per_Retry'), '00:00:10')
		
Select @daysOfHistoryToProcess = IsNull(@default_DaysToRescan, 15);

if @default_DATA_COMPRESSION not in ('NONE','ROW','PAGE') set @default_DATA_COMPRESSION = 'PAGE'

IF DATEPART(WEEKDAY,GETDATE()) in (1,7)
	BEGIN				
		-- If weekend, get new Max time value. Redo all processing if setting allows.
		IF @RescanOnWeekend = 1
			Begin
				IF DATEPART(WEEKDAY,GETDATE()) = 7 -- Saturday
				BEGIN
					SET @default_DaysToRescan = 0
				END
				ELSE IF DATEPART(WEEKDAY,GETDATE()) = 1 -- Sunday
				BEGIN
					SET @default_DaysToRescan = 1
				END		
				Select @daysOfHistoryToProcess = 3; -- Basically going to rescan everything so 3 is fine here.		
			End
		SELECT @maxRuntimeSeconds	= COALESCE((select cast(value as int) from dbo.Configuration WITH (NOLOCK) where configurationID = 'MNT_MaintenanceIndexes_MaxRunTime_Weekend'), 36000)
	END
ELSE
	BEGIN -- If it's not the weekend and we are only supposed to run on the weekend, exit if we've run within the past 7 days.	
		IF @MaintenanceIndexesOnlyRunOnWeekendSetting = 1 AND ABS(DATEDIFF(d, @LastRunDate, GETDATE())) <= 7
			BEGIN
				SELECT @ExitType = 2	
				GOTO CONDITIONAL_EXIT			
			End			
	END
	
-- Update last run date.	
UPDATE dbo.Configuration SET Value = GETDATE() WHERE ConfigurationId = 'MNT_MaintenanceIndexes_LastRunDate'

declare
	@now datetime = getdate()
	, @sql nvarchar(4000)
	, @txt nvarchar(4000)
	, @SchemaName varchar(256)
	, @object_id int
	, @TableName varchar(256)
	, @ObjectType varchar(11)
	, @Index_id int
	, @IndexName varchar(256)
	, @IndexType int
	, @partition_number int
	, @partition_count int
	-----
	, @currFragPercent decimal(9,5)
	, @currPageQty bigint
	, @idVal int
	, @val_PAD_INDEX varchar(32)
	, @val_STATISTICS_NORECOMPUTE varchar(32)
	, @val_SORT_IN_TEMPDB varchar(32)
	, @val_IGNORE_DUP_KEY varchar(32)
	, @val_ONLINE varchar(32)
	, @val_ALLOW_ROW_LOCKS varchar(32)
	, @val_ALLOW_PAGE_LOCKS varchar(32)
	, @val_FILLFACTOR varchar(32)
	, @val_DATA_COMPRESSION varchar(32)
	, @val_MAXDOP SMALLINT
	, @val_DaysToRescan SMALLINT
	, @val_FragPercent_Reorganize TINYINT
	, @val_FragPercent_Rebuild TINYINT
	, @isLob bit
	, @exitLoop bit = 0

if @insertIntoCML = 1
	BEGIN
		SET @eMessage = 'MNT_MaintenanceIndexes. MaxRunTime: ' + CONVERT(VARCHAR, @maxRuntimeSeconds) + '. DaysToRescan: ' + CONVERT(VARCHAR, @default_DaysToRescan) + '.'
		EXEC cardutil.LogIt @StartDt, 'MNT_MaintenanceIndexes', @@ProcID, 220, 1, 'I', @@ERROR, null, @eMessage, 0								
		SELECT @startDt = GETDATE();
	END

--=========================================================
--LIST ALL CANDIDATE INDEXES
--=========================================================
declare @tabIndex table
(
	SchemaName varchar(256)
	, [object_id] int
	, TableName varchar(256)
	, ObjectType varchar(11)
	, Index_id int
	, IndexName varchar(256)
	, IndexType int
	, partition_number int
	, partition_count int
	, val_PAD_INDEX varchar(32)
	, val_STATISTICS_NORECOMPUTE varchar(32)
	, val_SORT_IN_TEMPDB varchar(32)
	, val_IGNORE_DUP_KEY varchar(32)
	, val_ONLINE varchar(32)
	, val_ALLOW_ROW_LOCKS varchar(32)
	, val_ALLOW_PAGE_LOCKS varchar(32)
	, val_FILLFACTOR varchar(32)
	, val_DATA_COMPRESSION varchar(32)
	, val_MAXDOP varchar(32)
	, val_DaysToRescan SMALLINT
	, val_FragPercent_Reorganize TINYINT
	, val_FragPercent_Rebuild TINYINT
)

declare @tabTempVal table (i int) -- temp table for return value from dynamic SQL

set @sql = 
'
select X.*
, IRC.val_PAD_INDEX , IRC.val_STATISTICS_NORECOMPUTE, IRC.val_SORT_IN_TEMPDB, IRC.val_IGNORE_DUP_KEY, IRC.val_ONLINE
, IRC.val_ALLOW_ROW_LOCKS, IRC.val_ALLOW_PAGE_LOCKS, IRC.val_FILLFACTOR, IRC.val_DATA_COMPRESSION, IRC.val_MAXDOP
, IRC.DaysToRescan, IRC.FragPercent_Reorganize, IRC.FragPercent_Rebuild
from
(
SELECT 
	S.[name] as SchemaName
	, I.[object_id]
	, O.[name] as TableName
	, RTRIM(O.[type]) as ObjectType
	, I.index_id
	, I.[name] as IndexName
	, I.[type] as IndexType
	, P.partition_number
	, IndexPartitions.partition_count
FROM sys.indexes I with (nolock)
INNER JOIN sys.objects O with (nolock)
	ON I.[object_id] = O.[object_id] 
INNER JOIN sys.schemas S with (nolock)
	ON O.[schema_id] = S.[schema_id] 
LEFT OUTER JOIN .sys.partitions P with (nolock)
	ON I.[object_id] = P.[object_id] 
	AND I.index_id = P.index_id
LEFT OUTER JOIN 
	(
		SELECT [object_id], index_id, COUNT(*) AS partition_count 
		FROM sys.partitions with (nolock)
		GROUP BY [object_id], index_id
	) IndexPartitions 
	ON P.[object_id] = IndexPartitions.[object_id] 
	AND P.[index_id] = IndexPartitions.[index_id] 
WHERE 
	O.[type] IN(''U'',''V'') 
	AND O.is_ms_shipped = 0 
	AND I.[type] IN(1,2,3,4)
	AND I.is_disabled = 0 
	AND I.is_hypothetical = 0 
	and O.[name] not in (''IndexRebuildConfig'',''IndexRebuildHist'')
) X
left join cardutil.IndexRebuildConfig IRC with (nolock)
	on IRC.SchemaName = X.SchemaName
	and IRC.TableName = X.TableName
	and IRC.IndexName = X.IndexName
--join with history, and only pull records where NOT scanned within @default_DaysToRescan days
left join cardutil.IndexRebuildHist IRH with (nolock)
	on IRH.SchemaName = X.SchemaName
	and IRH.TableName = X.TableName
	and IRH.IndexName = X.IndexName
	and IRH.partition_number = X.partition_number
	and IRH.datStrStats >= dateadd(dd,isNull(IRC.DaysToRescan,' + cast(@default_DaysToRescan as varchar(11)) + ') * (-1),getdate())
where IRH.IndexName is null -- do not include these records -- previously scanned within window
order by X.SchemaName,X.TableName,X.IndexName,X.partition_number
'
if @outputSQL = 1 print @sql
if @executeSQL = 1 
	begin
		insert into @tabIndex 
			(
				SchemaName, [object_id], TableName, ObjectType, Index_id, IndexName, IndexType , partition_number, partition_count
				, val_PAD_INDEX, val_STATISTICS_NORECOMPUTE, val_SORT_IN_TEMPDB, val_IGNORE_DUP_KEY, val_ONLINE
				, val_ALLOW_ROW_LOCKS, val_ALLOW_PAGE_LOCKS, val_FILLFACTOR, val_DATA_COMPRESSION, val_MAXDOP
				, val_DaysToRescan, val_FragPercent_Reorganize, val_FragPercent_Rebuild
			)
		exec sp_executesql @sql
	end
--=========================================================
--CALCULATE FRAGMENTATION
--=========================================================
declare curIndex cursor local for
	select
		SchemaName, a.[object_id], TableName, ObjectType, Index_id, IndexName, IndexType , partition_number, partition_count
		, isnull(val_PAD_INDEX,@default_PAD_INDEX), isnull(val_STATISTICS_NORECOMPUTE,@default_STATISTICS_NORECOMPUTE), isnull(val_SORT_IN_TEMPDB,@default_SORT_IN_TEMPDB)
		, isnull(val_IGNORE_DUP_KEY,@default_IGNORE_DUP_KEY), isnull(val_ONLINE,@default_ONLINE)
		, isnull(val_ALLOW_ROW_LOCKS,@default_ALLOW_ROW_LOCKS), isnull(val_ALLOW_PAGE_LOCKS,@default_ALLOW_PAGE_LOCKS), isnull(val_FILLFACTOR,@default_FILLFACTOR)
		, isnull(val_DATA_COMPRESSION,@default_DATA_COMPRESSION), isnull(val_MAXDOP,@default_MAXDOP)
		, isnull(val_DaysToRescan,@default_DaysToRescan), isnull(val_FragPercent_Reorganize,@default_FragPercentReorganize), isnull(val_FragPercent_Rebuild,@default_FragPercentRebuild) 
	from @tabIndex a

open curIndex
		fetch next from curIndex into @SchemaName, @object_id, @TableName, @ObjectType, @Index_id, @IndexName, @IndexType , @partition_number, @partition_count
			, @val_PAD_INDEX , @val_STATISTICS_NORECOMPUTE, @val_SORT_IN_TEMPDB, @val_IGNORE_DUP_KEY, @val_ONLINE
			, @val_ALLOW_ROW_LOCKS, @val_ALLOW_PAGE_LOCKS, @val_FILLFACTOR, @val_DATA_COMPRESSION, @val_MAXDOP	
			, @val_DaysToRescan, @val_FragPercent_Reorganize, @val_FragPercent_Rebuild
while @@fetch_status = 0 and @exitLoop = 0
BEGIN
	---- reset deadlock counter for new batch ----
	SET @DeadlockTries = 0;
	
	WHILE @DeadlockTries <= @DeadLockTotalTries
	BEGIN
	
		BEGIN TRY
			-- JLK Ensure index still exists
			IF EXISTS (SELECT TOP 1 1
						FROM sys.indexes I with (nolock)
						INNER JOIN sys.objects O with (nolock)
							ON I.[object_id] = O.[object_id] 
						INNER JOIN sys.schemas S with (nolock)
							ON O.[schema_id] = S.[schema_id] 
						LEFT OUTER JOIN .sys.partitions P with (nolock)
							ON I.[object_id] = P.[object_id] 
							AND I.index_id = P.index_id
						LEFT OUTER JOIN 
							(
								SELECT [object_id], index_id, COUNT(*) AS partition_count 
								FROM sys.partitions with (nolock)
								GROUP BY [object_id], index_id
							) IndexPartitions 
							ON P.[object_id] = IndexPartitions.[object_id] 
							AND P.[index_id] = IndexPartitions.[index_id] 
						WHERE 
							O.[type] IN('U','V') 
							AND O.is_ms_shipped = 0 
							AND I.[type] IN(1,2,3,4)
							AND I.is_disabled = 0 
							AND I.is_hypothetical = 0 
							and O.[name] not in ('IndexRebuildConfig','IndexRebuildHist')	
							AND i.object_id = @object_id
							and i.index_id = @Index_id
							and p.Partition_Number = @partition_number)
			BEGIN				
				set @sql = 
				case 
					when @IndexType = 1 then 
						'SELECT COUNT(*) FROM sys.columns C with (nolock)
						INNER JOIN sys.types T with (nolock)
						ON C.system_type_id = T.user_type_id OR (C.user_type_id = T.user_type_id AND T.is_assembly_type = 1) 
						WHERE C.[object_id] = ' + CAST(@object_id AS nvarchar) + ' 
						AND (T.name IN(''xml'',''image'',''text'',''ntext'') OR T.name IN(''varchar'',''nvarchar'',''varbinary'') 
						AND (C.max_length = -1) OR (T.is_assembly_type = 1 AND C.max_length = -1))'
					when @IndexType = 2 then 
						'SELECT COUNT(*) FROM sys.index_columns IC with (nolock)
						INNER JOIN sys.columns C with (nolock) 
							ON IC.[object_id] = C.[object_id] 
							AND IC.column_id = C.column_id 
						INNER JOIN sys.types T with (nolock)
							ON C.system_type_id = T.user_type_id 
							OR (C.user_type_id = T.user_type_id AND T.is_assembly_type = 1) 
						WHERE IC.[object_id] = ' + CAST(@object_id AS nvarchar) 
							+ ' AND IC.index_id = ' + CAST(@Index_id AS nvarchar) 
							+ ' AND (
									T.[name] IN(''xml'',''image'',''text'',''ntext'') 
									OR (T.[name] IN(''varchar'',''nvarchar'',''varbinary'') AND C.max_length = -1) 
									OR (T.is_assembly_type = 1 AND C.max_length = -1)
									)'
					else 'select 0' end
				if @outputSQL = 1 print @sql
				if @executeSQL = 1 
					begin
							delete @tabTempVal
							insert into @tabTempVal (i)
						exec sp_executesql @sql
						set @isLob = (select max(case when i > 0 then 1 else 0 end) from @tabTempVal)
					end
				
				
				select @currFragPercent = max(avg_fragmentation_in_percent)
					   , @currpageQty= sum(page_count)
				from sys.dm_db_index_physical_stats(db_id(), @object_id, @Index_id, @partition_number, 'LIMITED')
				where alloc_unit_type_desc = 'IN_ROW_DATA'
					and index_level = 0
				
				
				IF @currFragPercent >= @val_FragPercent_Reorganize AND @currFragPercent < @val_FragPercent_Rebuild 
				   ---- reorganize may only run when Allow Page Locks is TRUE and the index is NOT Disabled
				   AND EXISTS (SELECT TOP 1 1 FROM sys.indexes with (nolock) WHERE object_id=@object_id AND index_id=@Index_id AND [allow_page_locks]=1 AND is_disabled=0)
					BEGIN
						SET @sql = 'alter index [' + @IndexName + '] on [' + @SchemaName + '].[' + @TableName + '] REORGANIZE '
								 + CASE WHEN @partition_count > 1 THEN ' PARTITION = ' + CAST(@partition_number AS VARCHAR(22)) ELSE '' END
					END
				ELSE IF @currFragPercent >= @val_FragPercent_Rebuild 
					BEGIN
						SET @sql = 
							'alter index [' + @IndexName + '] on [' + @SchemaName + '].[' + @TableName + '] rebuild '
							+ case when @partition_count > 1 then ' partition = ' + cast(@partition_number as varchar(22))
								else '' end
							+ ' with (' 
							+ 'SORT_IN_TEMPDB = ' + @val_SORT_IN_TEMPDB
							+ case when @partition_count > 1 then '' else ', PAD_INDEX = ' + @val_PAD_INDEX end
							+ case when @partition_count > 1 then '' else ', STATISTICS_NORECOMPUTE = ' + @val_STATISTICS_NORECOMPUTE end
							+ case when @partition_count > 1 then '' 
								when @isLob = 1 then ', ONLINE = OFF'
								else ', ONLINE = ' + @val_ONLINE end
							+ case when @partition_count > 1 then '' else ', ALLOW_ROW_LOCKS = ' + @val_ALLOW_ROW_LOCKS end
							+ case when @partition_count > 1 then '' else ', ALLOW_PAGE_LOCKS = ' + @val_ALLOW_PAGE_LOCKS end
							+ case when @partition_count > 1 then '' else ', FILLFACTOR = ' + @val_FILLFACTOR end
							+ case when @val_DATA_COMPRESSION is null then '' else ', DATA_COMPRESSION = ' + @val_DATA_COMPRESSION end
							+ ', MAXDOP = ' + CAST(@val_MAXDOP AS VARCHAR(30))
							+ ') '
						;
					END
				ELSE 
					BEGIN
						SET @sql = ''
					END
				
				
				insert into cardutil.IndexRebuildHist
					(SchemaName, TableName, IndexName, partition_number, sqlCommand, datEndStats, FragPercent, PageQty)
				values
					(@SchemaName, @TableName, @IndexName, @partition_number, @sql, GETDATE(), @currFragPercent, @currPageQty)
				;
				SELECT @idVal = SCOPE_IDENTITY();
			END
			ELSE
			BEGIN
				SET @eMessage = 'Index no longer exists! Not checking fragmentation for: ' + @SchemaName + '.' + @IndexName + ' Partition: ' + CONVERT(VARCHAR(50), @partition_number)
				EXEC cardutil.LogIt @StartDt, 'MNT_MaintenanceIndexes', @@ProcID, 220, 2, 'I', @@ERROR, null, @eMessage, 0								
				update cardutil.IndexRebuildHist set datEndStats = getdate(),FragPercent = 0, PageQty = 0, sqlCommand = '' -- << no SQL command stops rebuild.
				where IndexRebuildHist_ID = @idVal				
			END
			
			-------------------------------------
			---- exit deadlock / retry logic ----
			-------------------------------------
			BREAK;
			-------------------------------------
			-------------------------------------
		END TRY
		BEGIN CATCH
			IF XACT_STATE() = -1 OR @@TRANCOUNT > 0
				ROLLBACK TRAN
			
			IF ERROR_NUMBER() = 1205 -- Deadlock Error
				BEGIN
					SELECT @DeadlockTries += 1;
					IF (@DeadlockTries > @DeadLockTotalTries)
						BEGIN
							SELECT @eMessage = 'Exceeded deadlock retry attempts. Problem while inserting offer merchant details in ' + OBJECT_NAME(@@PROCID) + '.';
							SELECT @eMessage += ' Proc: ' + ISNULL(ERROR_PROCEDURE(),'MNT_MaintenanceIndexes') + ' Err#: ' + CONVERT(VARCHAR, ISNULL(ERROR_NUMBER(),-1)) + ' Line: ' + CONVERT(VARCHAR, ISNULL(ERROR_LINE(),-1)) + ' Msg: ' + ISNULL(ERROR_MESSAGE(),'Error Occurred')
							EXEC cardutil.LogIt @StartDt, 'MNT_MaintenanceIndexes', @@ProcID, 220, 98, 'E', @@ERROR, NULL, @eMessage, 0
							-- Break out of endless loop.
							BREAK;
						END
					ELSE
						BEGIN
							WAITFOR DELAY @DeadLockDelay;
							SELECT @eMessage = 'Hit Deadlock in ' + OBJECT_NAME(@@PROCID) + '.';
							EXEC cardutil.LogIt @StartDt, 'MNT_MaintenanceIndexes', @@ProcID, 220, 97, 'W', @@ERROR, NULL, @eMessage, 0
							-- keep going in loop.
							CONTINUE;
						END
				END
			ELSE
				BEGIN
					SELECT @eMessage = '--ERROR INSERTING INTO IndexRebuildHist ';
					SELECT @eMessage += ' Proc: ' + ISNULL(ERROR_PROCEDURE(),'MNT_MaintenanceIndexes') + ' Err#: ' + CONVERT(VARCHAR, ISNULL(ERROR_NUMBER(),-1)) + ' Line: ' + CONVERT(VARCHAR, ISNULL(ERROR_LINE(),-1)) + ' Msg: ' + ISNULL(ERROR_MESSAGE(),'Error Occurred')
					EXEC cardutil.LogIt @StartDt, 'MNT_MaintenanceIndexes', @@ProcID, 220, 98, 'I', @@ERROR, null, @eMessage, 0	
				END
				
			-- Break out of endless loop, only exits when other error handling sections were not hit.
			BREAK;
		END CATCH
	
	END ---- end block:: deadlock/retry

	fetch next from curIndex into @SchemaName, @object_id, @TableName, @ObjectType, @Index_id, @IndexName, @IndexType , @partition_number, @partition_count
		, @val_PAD_INDEX , @val_STATISTICS_NORECOMPUTE, @val_SORT_IN_TEMPDB, @val_IGNORE_DUP_KEY, @val_ONLINE
		, @val_ALLOW_ROW_LOCKS, @val_ALLOW_PAGE_LOCKS, @val_FILLFACTOR, @val_DATA_COMPRESSION, @val_MAXDOP
		, @val_DaysToRescan, @val_FragPercent_Reorganize, @val_FragPercent_Rebuild
	if dateadd(ss,@maxRuntimeSeconds,@now) < getdate() 
		begin
			print '--Graceful exit after ' + cast(@maxRuntimeSeconds as varchar(33)) + ' seconds. ============================='
			set @exitLoop = 1
			SELECT @ExitType = 1	
			GOTO CONDITIONAL_EXIT	
		end
END

--=========================================================
--REBUILD INDEXES:
--=========================================================
	declare curRebuild cursor local for
	select IndexRebuildHist_ID, sqlCommand
	from cardutil.IndexRebuildHist a with (nolock)
	where datStrStats >= dateadd(dd,isnull(@daysOfHistoryToProcess,0)*(-1),@now)
		and datStrRebuild is NULL				
		and indexrebuildhist_id = (select max(indexrebuildhist_id) from [cardutil].[IndexRebuildHist] b with (nolock) where a.schemaname = b.schemaname and a.TableName = b.TableName and a.IndexName = b.IndexName and a.partition_number = b.partition_number)
		AND len(sqlCommand) > 0
	order by pageQty --IndexRebuildHist_ID

	open curRebuild
		fetch next from curRebuild into @idVal,@sql

	while @@fetch_status = 0 and @exitLoop = 0
	BEGIN
		-- JLK Ensure index still exists. This can still be hit if indexes were scanned earlier and the prior run didn't finish. 
		IF EXISTS (SELECT TOP 1 1
					FROM sys.indexes I with (nolock)
					INNER JOIN sys.objects O with (nolock)
						ON I.[object_id] = O.[object_id] 
					INNER JOIN sys.schemas S with (nolock)
						ON O.[schema_id] = S.[schema_id] 
					LEFT OUTER JOIN .sys.partitions P with (nolock)
						ON I.[object_id] = P.[object_id] 
						AND I.index_id = P.index_id
					INNER JOIN cardutil.IndexRebuildHist ih
						ON ih.IndexName = i.name
							AND ih.SchemaName = s.name
							AND ih.partition_number = p.partition_number
					LEFT OUTER JOIN 
						(
							SELECT [object_id], index_id, COUNT(*) AS partition_count 
							FROM sys.partitions with (nolock)
							GROUP BY [object_id], index_id
						) IndexPartitions 
						ON P.[object_id] = IndexPartitions.[object_id] 
						AND P.[index_id] = IndexPartitions.[index_id] 
					WHERE 
						O.[type] IN('U','V') 
						AND O.is_ms_shipped = 0 
						AND I.[type] IN(1,2,3,4)
						AND I.is_disabled = 0 
						AND I.is_hypothetical = 0 
						and O.[name] not in ('IndexRebuildConfig','IndexRebuildHist')	
						AND ih.IndexRebuildHist_ID = @idVal)
		BEGIN				
	
			if @outputSQL = 1 and len(@sql) > 0 print @sql
			if @executeSQL = 1 and len(@sql) > 0 and @performRebuild = 1
				BEGIN				
					begin TRY
					SET @cStartDt = GETDATE();
					
					update cardutil.IndexRebuildHist set datStrRebuild = getdate() where IndexRebuildHist_ID = @idVal
					exec sp_executesql @sql				
					update cardutil.IndexRebuildHist set datEndRebuild = getdate() where IndexRebuildHist_ID = @idVal
					
					-- Log
					SET @cMessage = 'Executed: ' + @sql						
   					EXEC cardutil.LogIt @cStartDt, 'MNT_MaintenanceIndexes', @@ProcID, 220, 3, 'I', @@ERROR, NULL, @cMessage, 0
	   				
					end try
					begin catch						
						SELECT @eMessage = '--REBUILD INDEXES ERROR -- Message: ' + ERROR_MESSAGE() + ' -- Line: ' + cast(ERROR_LINE() as varchar(222)) + ' -- Error#: ' + cast(ERROR_NUMBER() as varchar(222))
						EXEC cardutil.LogIt @StartDt, 'MNT_MaintenanceIndexes', @@ProcID, 220, 98, 'I', @@ERROR, null, @eMessage, 0			
					end catch
				end
		END
		ELSE
		BEGIN
			SELECT @eMessage = 'Index no longer exists! Not rebuilding: ' + SchemaName + '.' + TableName + ' Index: ' + indexname + ' Partition: ' + CONVERT(VARCHAR(50), partition_number)
									from cardutil.IndexRebuildHist WITH (NOLOCK) WHERE IndexRebuildHist_ID = @idVal
			EXEC cardutil.LogIt @cStartDt, 'MNT_MaintenanceIndexes', @@ProcID, 220, 3, 'I', @@ERROR, null, @eMessage, 0			
			update cardutil.IndexRebuildHist set datStrRebuild = getdate(), datEndRebuild = getdate() where IndexRebuildHist_ID = @idVal		
		END
		
		fetch next from curRebuild into @idVal,@sql
		if dateadd(ss,@maxRuntimeSeconds,@now) < getdate() 
			begin
				print '--Graceful exit after ' + cast(@maxRuntimeSeconds as varchar(33)) + ' seconds. ============================='
				set @exitLoop = 1
				SELECT @ExitType = 1	
				GOTO CONDITIONAL_EXIT	
			END
	end
--=========================================================
--Additional LOGGING INTO CML
--=========================================================
if @LogHistoryDetails = 1
begin
	declare curMSG cursor local for
		select 'INDEX (Rebuild=' + case when datStrRebuild is not null then 'Y' else 'N' end
			+ '): [' + SchemaName + '].[' + TableName + '].[' + IndexName 
			+ '] || PartitionNum: ' + right('0000' + cast(Partition_number as varchar(11)),5)
			+ ' ~~ FragPerc: ' + right('0' + cast(cast(FragPercent as int) as varchar(44)),2)
			+ ' ~| PageQty: '  + right('00000000000' + cast(PageQty as varchar(22)),12)
			+ ' ^^ StatsSecs: ' + right('00000' + cast(isnull(datediff(ss,datStrStats,datEndStats),0) as varchar(22)),6)
			+ ' |~ RebuildSecs: ' + right('00000' + cast(isnull(datediff(ss,datStrRebuild,datEndRebuild),0) as varchar(22)),6)
			+ ' ^| SQL: ' + sqlCommand
		from
		(
		select SchemaName, TableName, IndexName, Partition_number, FragPercent, PageQty, datStrStats,datEndStats, datStrRebuild,datEndRebuild, sqlCommand
		from cardutil.IndexRebuildHist with (nolock)
		where isnull(datStrRebuild,datStrStats) >= @now
		) X
	open curMSG
	fetch next from curMSG into @txt
	while @@fetch_status = 0
		BEGIN
			SET @eMessage = @txt
			EXEC cardutil.LogIt @StartDt, 'MNT_MaintenanceIndexes', @@ProcID, 220, 4, 'I', @@ERROR, null, @eMessage, 0								

			fetch next from curMSG into @txt
		end
end

--=========================================================
--END
--=========================================================
if @insertIntoCML = 1
	BEGIN
		EXEC cardutil.LogIt @StartDt, 'MNT_MaintenanceIndexes', @@ProcID, 220, 99, 'I', @@ERROR, null, 'MNT_MaintenanceIndexes', 0								
	end
--------------------------
CONDITIONAL_EXIT:

	IF @ExitType = 1
		BEGIN
			SET @eMessage = 'MNT_MaintenanceIndexes exited after it reached its maximum run time limit of ' + CAST(@maxRuntimeSeconds AS VARCHAR) + ' seconds.'
			EXEC cardutil.LogIt @StartDt, 'MNT_MaintenanceIndexes', @@ProcID, 220, 99, 'I', @@ERROR, null, @eMessage, 0						
		END
	ELSE if @ExitType = 2		
		BEGIN
			SET @eMessage = 'MNT_MaintenanceIndexes did not run because it is not the weekend.'
			EXEC cardutil.LogIt @StartDt, 'MNT_MaintenanceIndexes', @@ProcID, 220, 99, 'I', @@ERROR, null, @eMessage, 0								
		END

end
GO
