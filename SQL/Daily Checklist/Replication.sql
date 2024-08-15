

Declare @Sname nvarchar(150) = (select @@servername)

IF @Sname = '[SERVERNAME]'
 
 begin

	select 
		@Sname as "Server",
		KeyName,
		ConfigValue,
		getdate() as "Time this was executed"
	from [DATABASE]_Replication.dbo.configuration
	where keyname = 'Last_Replication_Heartbeat'

end

If @Sname = '[SERVERNAME]'

 begin

	select 
		@Sname as "Server",
		KeyName,
		ConfigValue,
		getdate() as "Time this was executed"
	from OMS_REPLICATION.dbo.configuration
	where keyname = 'Last_Replication_Heartbeat'

end

If @Sname = '[SERVERNAME2]'

 begin

	select 
		@Sname as "Server",
		KeyName,
		ConfigValue,
		getdate() as "Time this was executed"
	from OMS.dbo.configuration
	where keyname = 'Last_Replication_Heartbeat'

end