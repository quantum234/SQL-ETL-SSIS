use SQLOPS
go

--SELECT DB_NAME() AS DataBaseName

  exec Daily_BackupReport
  exec Daily_CPU
  exec Daily_DiskSpace
  exec Daily_JobFailures
  exec Daily_LogSize
  exec Daily_Replication
  exec Daily_SqlServerAgentErrorLog
  exec Daily_SqlServerErrorLog
  exec Daily_WhoIsActive

  /*




  */