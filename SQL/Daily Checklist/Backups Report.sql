--I customized the query slightly to display size in MB and changed the datediff function to minutes from seconds. I also removed a number of redundant and null fields:

SELECT 
d.name ,

MAX (d. recovery_model_desc) AS "Recovery Model",
           
is_Password_Protected , --Backups Encrypted:

--Last Full Database Backup:
MAX (CASE
    WHEN TYPE = 'D' THEN backup_start_date
    ELSE NULL
END) AS [Last Full Database Backup] ,

--Last Differential Log Backup:
MAX (CASE
WHEN TYPE = 'I' THEN backup_start_date
ELSE NULL
END) AS [Last Differential Backup] ,

--Last Transaction Log Backup:
MAX (CASE
WHEN TYPE = 'L' THEN backup_start_date
ELSE NULL
END) AS [Last Transaction Log Backup] ,

--How Often are Transaction Logs Backed Up:
DATEDIFF (DAY , MIN (CASE WHEN TYPE = 'L' THEN backup_start_date ELSE 0 END), 
MAX (CASE WHEN TYPE = 'L' THEN backup_start_date ELSE 0 END)) / NULLIF (SUM (CASE WHEN TYPE = 'I' THEN 1 ELSE 0 END), 0) [Logs BackUp count] ,

--Average backup times:
SUM (CASE
         WHEN TYPE = 'D' THEN DATEDIFF (minute , backup_start_date, Backup_finish_date)
         ELSE 0
     END) / NULLIF (SUM (CASE WHEN TYPE = 'D' THEN 1 ELSE 0 END), 0) AS [Average DATABASE FULL Backup Time(min)] , 

SUM (CASE
WHEN TYPE = 'I' THEN DATEDIFF (minute , backup_start_date, Backup_finish_date)
ELSE 0
END) / NULLIF (SUM (CASE WHEN TYPE = 'I' THEN 1 ELSE 0 END), 0) AS [Average Differential Backup Time(min)] , 

SUM (CASE
WHEN TYPE = 'L' THEN DATEDIFF (minute , backup_start_date, Backup_finish_date)
ELSE 0
END) / NULLIF (SUM (CASE WHEN TYPE = 'L' THEN 1 ELSE 0 END), 0) AS [Average Log Backup Time(min)] ,
              
MAX (CASE
WHEN TYPE = 'D' THEN cast (backup_size /1024/1024 as numeric (10,2))
ELSE 0
END) AS [Database Full Backup Size(mb)] ,
                   
SUM (CASE
    WHEN TYPE = 'L' THEN cast (backup_size /1024/1024 as numeric (10,2))
    ELSE 0
END) / NULLIF (SUM (CASE WHEN TYPE = 'L' THEN 1 ELSE 0 END), 0) AS [Average Transaction Log Backup Size(mb)] ,
                                  
--Backup compression?:
CASE
       WHEN SUM (backup_size - compressed_backup_size) <> 0 THEN 'yes'
       ELSE 'no'
END AS [Backups Compressed]

FROM master .sys.databases d
LEFT OUTER JOIN msdb. dbo. backupset b ON d. name = b. database_name
WHERE d.database_id NOT IN (2,
                            3)
GROUP BY d. name ,
         
is_Password_Protected --HAVING MAX(b.Backup_finish_date) <= DATEADD(dd, -7, GETDATE()) ;


