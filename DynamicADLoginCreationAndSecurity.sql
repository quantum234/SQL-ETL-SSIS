/**

--Need to add login and permission for the db_ssisadmin
SERVERNAME_INSTANCENAME_db_ssisadmin

USE [msdb]
GO
CREATE USER [DOMAIN\USER] FOR LOGIN [DOMAIN\USER]
GO
USE [msdb]
GO
ALTER ROLE [db_ssisadmin] ADD MEMBER [DOMAIN\USER]
GO


*/


USE [master]
set nocount on 
go

declare
	 @servername as nvarchar(128) = (select @@SERVERNAME)
	,@instnacename as nvarchar(128) = null
	,@sql as nvarchar(max)=''
	,@role as nvarchar(30)='db_reader'
	,@DBName as nvarchar(128)=''
	,@2008 as bit=0
	,@EXEC as bit = 0			--0 = DO NOT EXECUTE any sql, 1 = EXECUTE all sql

if PATINDEX('%SQL Server 2008%', @@version)<>0
BEGIN
	set @2008=1
END


declare @ADSecurityGroupTypes TABLE(name varchar(30) NOT NULL)
insert into @ADSecurityGroupTypes values ('sysadmin'),('db_datareader'),('db_datawriter'),('db_ddladmin'),('db_owner'),('db_executor'),('SQLAgentOperatorRole')

if OBJECT_ID('tempdb..#UserDBs') is not null drop table #UserDBs
create table #UserDBs ([name] nvarchar(128) NOT NULL)



--Separate out the server and instance name if there is one
if PATINDEX('%\%', @servername) <> 0
	begin
		select @instnacename  = right(@servername, len(@servername) - patindex('%\%', @servername))
		select @servername = left(@servername, patindex('%\%', @servername)-1)
	end

while exists (select 1 from @ADSecurityGroupTypes)
BEGIN

	select top 1 @role = [name] from @ADSecurityGroupTypes
	
	select @sql = 'IF NOT EXISTS (SELECT * FROM sys.server_principals WHERE name = N''DOMAIN\' + @servername + '_'
	if @instnacename is not null select @sql = @sql + @instnacename + '_'
	select @sql = @sql + @role + ''')' + char(13)

	select @sql = @sql + 'CREATE LOGIN [DOMAIN\' + @servername + '_'
	if @instnacename is not null select @sql = @sql + @instnacename + '_'
	select @sql = @sql + @role
	select @sql = @sql + '] FROM WINDOWS WITH DEFAULT_DATABASE=[master]'

	print @sql
	if @EXEC = 1 exec (@sql)

	delete @ADSecurityGroupTypes where [name] = @role

END

print char(13)

--public:		Provision securiyt to public
select @sql = 'USE master;GRANT VIEW ANY DEFINITION TO PUBLIC';print @sql;exec (@sql)
select @sql = 'GRANT SHOWPLAN TO PUBLIC';print @sql;exec (@sql)

print char(13)

--sysadmin:		Provision security for [sysadmin]
select @sql = 'IF EXISTS (SELECT * FROM sys.server_principals WHERE name = N''DOMAIN\' + @servername + '_'
if @instnacename is not null select @sql = @sql + @instnacename + '_'
select @sql = @sql + 'sysadmin'')' + char(13)

if @2008=0
	BEGIN
		select @sql = @sql + 'ALTER SERVER ROLE [sysadmin] ADD MEMBER [DOMAIN\' + @servername + '_'
		if @instnacename is not null select @sql = @sql + @instnacename + '_'
		select @sql = @sql + 'sysadmin'
		select @sql = @sql + ']'
	END
ELSE IF @2008=1
	BEGIN
		--EXEC master..sp_addsrvrolemember @loginame = N'DOMAIN\sysadmin', @rolename = N'sysadmin'
		select @sql = @sql + 'EXEC master..sp_addsrvrolemember @loginame = N''DOMAIN\' + @servername + '_'
		if @instnacename is not null select @sql = @sql + @instnacename + '_'
		select @sql = @sql + 'sysadmin'
		select @sql = @sql + ''', @rolename = N''sysadmin'''
	END


print @sql
if @EXEC = 1 exec (@sql)

print char(13)


--db_datareader
print '--db_datareader'
delete #UserDBs
insert into #UserDBs select [name] from sys.databases where database_id>4 and [state]=0 order by [name]



/*
Need to add this to the db_datareader role so everyone can view job info in the agent

USE [msdb]
GO
CREATE USER [DOMAIN\AccountManagers] FOR LOGIN [DOMAIN\AccountManagers]
GO
USE [msdb]
GO
ALTER ROLE [SQLAgentReaderRole] ADD MEMBER [DOMAIN\AccountManagers]
GO


*/



while exists (select 1 from #UserDBs)
BEGIN

	select @DBName = [name] from #UserDBs

	select @sql = 'USE [' + @DBName + ']' + char(13)
	select @sql = @sql + 'IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = N''DOMAIN\' + @servername + '_'
	if @instnacename is not null select @sql = @sql + @instnacename + '_'
	select @sql = @sql + 'db_datareader'')' + char(13)

	select @sql = @sql + 'CREATE USER [DOMAIN\' + @servername + '_'
	if @instnacename is not null select @sql = @sql + @instnacename + '_'
	select @sql = @sql + 'db_datareader] FOR LOGIN [DOMAIN\' + @servername + '_'
	if @instnacename is not null select @sql = @sql + @instnacename + '_'

	if @2008=0
		begin
			select @sql = @sql + 'db_datareader] WITH DEFAULT_SCHEMA=[dbo]' + char(13)
			select @sql = @sql + 'ALTER ROLE [db_datareader] ADD MEMBER [DOMAIN\' + @servername + '_'
			if @instnacename is not null select @sql = @sql + @instnacename + '_'
			select @sql = @sql + 'db_datareader]' + char(13)
		end
	else if @2008=1
		begin
			select @sql = @sql + 'db_datareader]' + char(13)
			select @sql = @sql + 'EXEC sp_addrolemember N''db_datareader'', N''DOMAIN\' + @servername + '_'
			if @instnacename is not null select @sql = @sql + @instnacename + '_'
			select @sql = @sql + 'db_datareader''' + char(13)
		end

	print @sql
	if @EXEC = 1 exec (@sql)

	delete #UserDBs where [name] = @DBName
END


--db_datawriter
print '--db_datawriter'
delete #UserDBs
insert into #UserDBs select [name] from sys.databases where database_id>4 and [state]=0 order by [name]

while exists (select 1 from #UserDBs)
BEGIN
	select @DBName = [name] from #UserDBs

	select @sql = 'USE [' + @DBName + ']' + char(13)
	select @sql = @sql + 'IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = N''DOMAIN\' + @servername + '_'
	if @instnacename is not null select @sql = @sql + @instnacename + '_'
	select @sql = @sql + 'db_datawriter'')' + char(13)

	select @sql = @sql + 'CREATE USER [DOMAIN\' + @servername + '_'
	if @instnacename is not null select @sql = @sql + @instnacename + '_'
	select @sql = @sql + 'db_datawriter] FOR LOGIN [DOMAIN\' + @servername + '_'
	if @instnacename is not null select @sql = @sql + @instnacename + '_'

	if @2008=0
		begin
			select @sql = @sql + 'db_datawriter] WITH DEFAULT_SCHEMA=[dbo]' + char(13)
			select @sql = @sql + 'ALTER ROLE [db_datawriter] ADD MEMBER [DOMAIN\' + @servername + '_'
			if @instnacename is not null select @sql = @sql + @instnacename + '_'
			select @sql = @sql + 'db_datawriter]' + char(13)
			select @sql = @sql + 'ALTER ROLE [db_datareader] ADD MEMBER [DOMAIN\' + @servername + '_'
			if @instnacename is not null select @sql = @sql + @instnacename + '_'
			select @sql = @sql + 'db_datawriter]' + char(13)
		end
	else if @2008=1
		begin
			--EXEC sp_addrolemember N'db_datareader', N'DOMAIN\CDWQAUAT_CDWQA1_db_datawriter'
			select @sql = @sql + 'db_datawriter]' + char(13)
			select @sql = @sql + 'EXEC sp_addrolemember N''db_datareader'', N''DOMAIN\' + @servername + '_'
			if @instnacename is not null select @sql = @sql + @instnacename + '_'
			select @sql = @sql + 'db_datawriter''' + char(13)
			select @sql = @sql + 'EXEC sp_addrolemember N''db_datawriter'', N''DOMAIN\' + @servername + '_'
			if @instnacename is not null select @sql = @sql + @instnacename + '_'
			select @sql = @sql + 'db_datawriter''' + char(13)
		end

	print @sql
	if @EXEC = 1 exec (@sql)

	delete #UserDBs where [name] = @DBName

END


--db_ddladmin
print '--db_ddladmin'
delete #UserDBs
insert into #UserDBs select [name] from sys.databases where database_id>4 and [state]=0 order by [name]

while exists (select 1 from #UserDBs)
BEGIN
	select @DBName = [name] from #UserDBs

	select @sql = 'USE [' + @DBName + ']' + char(13)
	select @sql = @sql + 'IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = N''DOMAIN\' + @servername + '_'
	if @instnacename is not null select @sql = @sql + @instnacename + '_'
	select @sql = @sql + 'db_ddladmin'')' + char(13)

	select @sql = @sql + 'CREATE USER [DOMAIN\' + @servername + '_'
	if @instnacename is not null select @sql = @sql + @instnacename + '_'
	select @sql = @sql + 'db_ddladmin] FOR LOGIN [DOMAIN\' + @servername + '_'
	if @instnacename is not null select @sql = @sql + @instnacename + '_'

	--if @2008=0
	--	begin
	--	end
	--else if @2008=1
	--	begin
	--		--EXEC sp_addrolemember N'db_datareader', N'DOMAIN\CDWQAUAT_CDWQA1_db_datawriter'
	--	end

	if @2008=0
		begin
			select @sql = @sql + 'db_ddladmin] WITH DEFAULT_SCHEMA=[dbo]' + char(13)
			select @sql = @sql + 'ALTER ROLE [db_ddladmin] ADD MEMBER [DOMAIN\' + @servername + '_'
			if @instnacename is not null select @sql = @sql + @instnacename + '_'
			select @sql = @sql + 'db_ddladmin]' + char(13)
			select @sql = @sql + 'ALTER ROLE [db_datawriter] ADD MEMBER [DOMAIN\' + @servername + '_'
			if @instnacename is not null select @sql = @sql + @instnacename + '_'
			select @sql = @sql + 'db_ddladmin]' + char(13)
			select @sql = @sql + 'ALTER ROLE [db_datareader] ADD MEMBER [DOMAIN\' + @servername + '_'
			if @instnacename is not null select @sql = @sql + @instnacename + '_'
			select @sql = @sql + 'db_ddladmin]' + char(13)
		end
	else if @2008=1
		begin
			--EXEC sp_addrolemember N'db_datareader', N'DOMAIN\CDWQAUAT_CDWQA1_db_datawriter'
			select @sql = @sql + 'db_ddladmin]' + char(13)
			select @sql = @sql + 'EXEC sp_addrolemember N''db_datareader'', N''DOMAIN\' + @servername + '_'
			if @instnacename is not null select @sql = @sql + @instnacename + '_'
			select @sql = @sql + 'db_ddladmin''' + char(13)
			select @sql = @sql + 'EXEC sp_addrolemember N''db_datawriter'', N''DOMAIN\' + @servername + '_'
			if @instnacename is not null select @sql = @sql + @instnacename + '_'
			select @sql = @sql + 'db_ddladmin''' + char(13)
			select @sql = @sql + 'EXEC sp_addrolemember N''db_ddladmin'', N''DOMAIN\' + @servername + '_'
			if @instnacename is not null select @sql = @sql + @instnacename + '_'
			select @sql = @sql + 'db_ddladmin''' + char(13)

		end
	
	

	print @sql
	if @EXEC = 1 exec (@sql)

	delete #UserDBs where [name] = @DBName

END


--db_owner
print '--db_owner'
delete #UserDBs
insert into #UserDBs select [name] from sys.databases where database_id>4 and [state]=0 order by [name]

while exists (select 1 from #UserDBs)
BEGIN
	select @DBName = [name] from #UserDBs

	select @sql = 'USE [' + @DBName + ']' + char(13)
	select @sql = @sql + 'IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = N''DOMAIN\' + @servername + '_'
	if @instnacename is not null select @sql = @sql + @instnacename + '_'
	select @sql = @sql + 'db_owner'')' + char(13)

	select @sql = @sql + 'CREATE USER [DOMAIN\' + @servername + '_'
	if @instnacename is not null select @sql = @sql + @instnacename + '_'
	select @sql = @sql + 'db_owner] FOR LOGIN [DOMAIN\' + @servername + '_'
	if @instnacename is not null select @sql = @sql + @instnacename + '_'

	--if @2008=0
	--	begin
	--	end
	--else if @2008=1
	--	begin
	--		--EXEC sp_addrolemember N'db_datareader', N'DOMAIN\CDWQAUAT_CDWQA1_db_datawriter'
	--	end

	if @2008=0
		begin
			select @sql = @sql + 'db_owner] WITH DEFAULT_SCHEMA=[dbo]' + char(13)
			select @sql = @sql + 'ALTER ROLE [db_owner] ADD MEMBER [DOMAIN\' + @servername + '_'
			if @instnacename is not null select @sql = @sql + @instnacename + '_'
			select @sql = @sql + 'db_owner]' + char(13)
		end
	else if @2008=1
		begin
			--EXEC sp_addrolemember N'db_datareader', N'DOMAIN\CDWQAUAT_CDWQA1_db_datawriter'
			select @sql = @sql + 'db_owner]' + char(13)
			select @sql = @sql + 'EXEC sp_addrolemember N''db_owner'', N''DOMAIN\' + @servername + '_'
			if @instnacename is not null select @sql = @sql + @instnacename + '_'
			select @sql = @sql + 'db_owner''' + char(13)
		end

	print @sql
	if @EXEC = 1 exec (@sql)

	delete #UserDBs where [name] = @DBName

END


--db_executor
print '--db_executor'
delete #UserDBs
insert into #UserDBs select [name] from sys.databases where database_id>4 and [state]=0 order by [name]

while exists (select 1 from #UserDBs)
BEGIN
	select @DBName = [name] from #UserDBs

	select @sql = 'USE [' + @DBName + ']' + char(13)
	select @sql = @sql + 'IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = N''DOMAIN\' + @servername + '_'
	if @instnacename is not null select @sql = @sql + @instnacename + '_'
	select @sql = @sql + 'db_executor'')' + char(13)

	select @sql = @sql + 'CREATE USER [DOMAIN\' + @servername + '_'
	if @instnacename is not null select @sql = @sql + @instnacename + '_'
	select @sql = @sql + 'db_executor] FOR LOGIN [DOMAIN\' + @servername + '_'
	if @instnacename is not null select @sql = @sql + @instnacename + '_'

	if @2008=0
		select @sql = @sql + 'db_executor] WITH DEFAULT_SCHEMA=[dbo]' + char(13)
	else if @2008=1
		select @sql = @sql + 'db_executor]' + char(13)

	select @sql = @sql + 'GRANT EXECUTE TO [DOMAIN\' + @servername + '_'
	if @instnacename is not null select @sql = @sql + @instnacename + '_'
	select @sql = @sql + 'db_executor]' + char(13)
	

	print @sql
	if @EXEC = 1 exec (@sql)

	delete #UserDBs where [name] = @DBName

END

--SQLAgentOperatorRole
print '--SQLAgentOperatorRole'
delete #UserDBs
insert into #UserDBs select [name] from sys.databases where [name]='msdb' order by [name]	--MSDN only

while exists (select 1 from #UserDBs)
BEGIN
	select @DBName = [name] from #UserDBs

	select @sql = 'USE [' + @DBName + ']' + char(13)
	select @sql = @sql + 'IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = N''DOMAIN\' + @servername + '_'
	if @instnacename is not null select @sql = @sql + @instnacename + '_'
	select @sql = @sql + 'SQLAgentOperatorRole'')' + char(13)

	select @sql = @sql + 'CREATE USER [DOMAIN\' + @servername + '_'
	if @instnacename is not null select @sql = @sql + @instnacename + '_'
	select @sql = @sql + 'SQLAgentOperatorRole] FOR LOGIN [DOMAIN\' + @servername + '_'
	if @instnacename is not null select @sql = @sql + @instnacename + '_'

	--if @2008=0
	--	begin
	--	end
	--else if @2008=1
	--	begin
	--		--EXEC sp_addrolemember N'db_datareader', N'DOMAIN\CDWQAUAT_CDWQA1_db_datawriter'
	--	end

	if @2008=0
		begin
			select @sql = @sql + 'SQLAgentOperatorRole] WITH DEFAULT_SCHEMA=[dbo]' + char(13)
			select @sql = @sql + 'ALTER ROLE [SQLAgentOperatorRole] ADD MEMBER [DOMAIN\' + @servername + '_'
			if @instnacename is not null select @sql = @sql + @instnacename + '_'
			select @sql = @sql + 'SQLAgentOperatorRole]' + char(13)
		end
	else if @2008=1
		begin
			--EXEC sp_addrolemember N'db_datareader', N'DOMAIN\CDWQAUAT_CDWQA1_db_datawriter'
			select @sql = @sql + 'SQLAgentOperatorRole]' + char(13)
			select @sql = @sql + 'EXEC sp_addrolemember N''SQLAgentOperatorRole'', N''DOMAIN\' + @servername + '_'
			if @instnacename is not null select @sql = @sql + @instnacename + '_'
			select @sql = @sql + 'SQLAgentOperatorRole''' + char(13)
		end

	print @sql
	if @EXEC = 1 exec (@sql)

	delete #UserDBs where [name] = @DBName

END




--Output SQL execution status
if @EXEC = 1
	PRINT '--SQL WAS EXECUTED'
else
	PRINT '--SQL was NOT executed'
