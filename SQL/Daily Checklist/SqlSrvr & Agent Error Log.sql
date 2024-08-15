
-- SqlServer and Agent error log check for failure over last day
declare 
	@StartDt date = convert(date,getdate()-1),
	@EndDt   date = convert(date,getdate())

-- Reads current SQL Server error log
EXEC master..xp_ReadErrorLog 0,1, "Failed",NULL,@StartDt, @EndDt,"DESC"
go

declare 
	@StartDt1 date = convert(date,getdate()-1),
	@EndDt1  date = convert(date,getdate())
-- Reads current SQL Server Agent error log
EXEC xp_ReadErrorLog 0,2, "Failed",NULL,@StartDt1, @EndDt1,"DESC"


declare 
	@StartDt2 date = convert(date,getdate()-1),
	@EndDt2   date = convert(date,getdate())

-- Reads current SQL Server error log
EXEC master..xp_ReadErrorLog 0,1, "Error",NULL,@StartDt2, @EndDt2,"DESC"
go

declare 
	@StartDt3 date = convert(date,getdate()-1),
	@EndDt3   date = convert(date,getdate())
-- Reads current SQL Server Agent error log
EXEC xp_ReadErrorLog 0,2, "Error",NULL,@StartDt3, @EndDt3,"DESC"
