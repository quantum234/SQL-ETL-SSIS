	   --- CREATE WhoIsActive table SCHEMA 
	   PRINT 'USE PerfStats'
	   PRINT 'If EXISTS(select * from sys.tables where name = ''WhoIsActive'') DROP TABLE [WhoISActive]'
	   declare @schema VARCHAR(MAX)
	   exec perfstats.dbo.sp_WhoIsActive 
			   @get_plans = 1, 
			   @get_locks = 1, 
			   @get_task_info = 1, 
			   @get_avg_time = 1, 
			   @delta_interval = 1, 
			   @format_output = 0, 
			   @get_outer_command = 1,
			   @find_block_leaders = 1,
			   @return_schema = 1,
			   @schema = @schema OUTPUT
	   SELECT REPLACE(@schema,'<table_name>','WhoIsActive')




--------------------------

exec dbo.sp_WhoIsActive 
		@get_plans = 1, 
		@get_locks = 1, 
		@get_task_info = 1, 
		@get_avg_time = 1, 
		@delta_interval = 1, 
		@format_output = 0, 
		@find_block_leaders = 1,
		@get_outer_command = 1,
		@destination_table = '[sqlops].[dbo].[WhoIsActive]'

--------------------------

	   IF EXISTS(SELECT * FROM msdb.dbo.sysjobs WHERE name like 'sp_WhoIsActive Collection%' and enabled = 0)
	   EXEC msdb.dbo.sp_update_job
		  @job_name = N'sp_WhoIsActive Collection - Stopped',
		  @new_name = N'sp_WhoIsActive Collection - Running',
		  @description = N'Start Collecting.',
		  @enabled = 1 ;
	   GO

----------------------------
