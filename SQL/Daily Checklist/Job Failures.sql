
--  Failed jobs over last day 
use msdb
go

declare @last30days int

set @last30days = convert(varchar(10), dateadd(day, -1, getdate()),112);

if ISDATE(@last30days) = 1
	exec msdb..sp_help_jobhistory @start_run_date = @last30days
			   ,@run_status = 0