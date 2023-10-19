/*
20160712
TEAM DBA

DBA Daily checklist queries. This script is designed to be a guide in order to complete the daily checklist. 

Currently a DBA resource will connect to each domain and run a series of stored procedures which live on the [SQLOPS] database on each respective server.

*/

USE SQLOPS
GO

exec Daily_BackupReport
exec Daily_CPU
exec Daily_DiskSpace
exec Daily_JobFailures
exec Daily_LogSize
exec Daily_Replication
exec Daily_SqlServerAgentErrorLog
exec Daily_SqlServerErrorLog
exec Daily_WhoIsActive
Exec Daily_JobOwner --please note this only exists on the XX domain currently