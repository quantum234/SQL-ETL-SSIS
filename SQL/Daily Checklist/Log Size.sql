


	declare @LogSize table  (DBName varchar(max), LogSize int, LogSpace int, Status tinyint)

	insert into @LogSize
	EXEC ('DBCC SQLPERF(LOGSPACE);')

	Select 
	DBName,
	LogSize/1024  as "Log Size GB",
	LogSpace as "Log Space Used %",
	Status
	from @LogSize