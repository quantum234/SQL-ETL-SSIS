select

  spid
 ,status
 ,hostname
 ,program_name
 ,cmd
 ,cpu
 ,physical_io
 ,blocked
 ,dbid
 ,convert(sysname, rtrim(loginame))
        as loginname
 ,spid as 'spid_sort'

 ,  substring( convert(varchar,last_batch,111) ,6  ,5 ) + ' '
  + substring( convert(varchar,last_batch,113) ,13 ,8 )
       as 'last_batch_char'
 ,request_id

      --into    tb1_sysprocesses
      from [XXXXXXXX].master.sys.sysprocesses with (nolock)
	  where len (hostname) > 1

	  select * from  [XXXXXXXX].master.sys.sysprocesses

	  select * from [XXXXXXXX].master.sys.sysprocesses with (nolock)
	   where len (hostname) > 1
--------------------------------------
--Create table

select
  spid
 ,status
 ,hostname
 ,program_name
 ,cmd
 ,cpu
 ,physical_io
 ,blocked
 ,dbid
 ,convert(sysname, rtrim(loginame))
        as loginname
 ,spid as 'spid_sort'

 ,  substring( convert(varchar,last_batch,111) ,6  ,5 ) + ' '
  + substring( convert(varchar,last_batch,113) ,13 ,8 )
       as 'last_batch_char'
 ,request_id
 , GETDATE() as InsertDate
INTO [sqlops].dbo.PS_LoginAuditing
from sys.sysprocesses with (nolock)
where len (hostname) > 1

------------------------------------------
--Insert from CORPJUMP01
insert into [sqlops].dbo.PS_LoginAuditing 
select spid
 ,status
 ,hostname
 ,program_name
 ,cmd
 ,cpu
 ,physical_io
 ,blocked
 ,dbid
 ,convert(sysname, rtrim(loginame))
        as loginname
 ,spid as 'spid_sort'
 ,  substring( convert(varchar,last_batch,111) ,6  ,5 ) + ' '
  + substring( convert(varchar,last_batch,113) ,13 ,8 )
       as 'last_batch_char'
 ,request_id
 , GETDATE() as InsertDate
from sys.sysprocesses with (nolock)
where len (hostname) > 1

-----------------------------------------------

insert into [sqlops].dbo.PS_LoginAuditing 
select spid
 ,status
 ,hostname
 ,program_name
 ,cmd
 ,cpu
 ,physical_io
 ,blocked
 ,dbid
 ,convert(sysname, rtrim(loginame))
        as loginname
 ,spid as 'spid_sort'
 ,  substring( convert(varchar,last_batch,111) ,6  ,5 ) + ' '
  + substring( convert(varchar,last_batch,113) ,13 ,8 )
       as 'last_batch_char'
 ,request_id
 , GETDATE() as InsertDate
from [XXXXXXXX].master.sys.sysprocesses with (nolock)
where len (hostname) > 1


------------------------------------------------

insert into [sqlops].dbo.PS_LoginAuditing 
select spid
 ,status
 ,hostname
 ,program_name
 ,cmd
 ,cpu
 ,physical_io
 ,blocked
 ,dbid
 ,convert(sysname, rtrim(loginame))
        as loginname
 ,spid as 'spid_sort'
 ,  substring( convert(varchar,last_batch,111) ,6  ,5 ) + ' '
  + substring( convert(varchar,last_batch,113) ,13 ,8 )
       as 'last_batch_char'
 ,request_id
 , GETDATE() as InsertDate
from [XXXXXXXX].master.sys.sysprocesses with (nolock)
where len (hostname) > 1

------------------------------------------------------------

insert into [sqlops].dbo.PS_LoginAuditing 
select spid
 ,status
 ,hostname
 ,program_name
 ,cmd
 ,cpu
 ,physical_io
 ,blocked
 ,dbid
 ,convert(sysname, rtrim(loginame))
        as loginname
 ,spid as 'spid_sort'
 ,  substring( convert(varchar,last_batch,111) ,6  ,5 ) + ' '
  + substring( convert(varchar,last_batch,113) ,13 ,8 )
       as 'last_batch_char'
 ,request_id
 , GETDATE() as InsertDate
from [XXXXXXXX].master.sys.sysprocesses with (nolock)
where len (hostname) > 1