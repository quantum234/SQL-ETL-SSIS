SELECT so.name, LeftRange =CONVERT(INT,left_prv.value), RightRange=CONVERT(INT,right_prv.value), ps.partition_number, ps.partition_id, ps.row_count,FNPartition= $PARTITION.[partition_function_DBO_TRXN_DayID_RR](CONVERT(INT,left_prv.value)) --Select *
	FROM sys.dm_db_partition_stats ps
	JOIN sys.objects so ON (ps.object_id = so.object_id)
	LEFT JOIN sys.partition_functions pf ON  (pf.name = 'partition_function_DBO_TRXN_DayID_RR')
	--LEFT JOIN sys.partition_range_values rv ON (rv.function_id = pf.function_id AND  ps.partition_number = rv.boundary_id )
	LEFT JOIN sys.partition_range_values left_prv ON left_prv.function_id = pf.function_id AND left_prv.boundary_id + 1 = ps.partition_number
	LEFT JOIN sys.partition_range_values right_prv ON right_prv.function_id = pf.function_id AND right_prv.boundary_id = ps.partition_number
	where so.name = 'trxn' 



--SELECT t.* 
--		INTO sqlops.dbo.Trxn_Tmp
--		FROM dbo.Trxn t
--		Where dayid >= 20190129  

select count(*) from sqlops.dbo.trxn_tmp
--209467

--begin tran

--Delete dbo.Trxn 
--from dbo.Trxn t
--join sqlops.dbo.Trxn_tmp x on x.transactionid = t.transactionId and x.dayid = t.dayid  

--commit tran

--ALTER PARTITION SCHEME partition_scheme_DBO_TRXN_DayID_RR NEXT USED DBO_TRXN
--ALTER PARTITION FUNCTION partition_function_DBO_TRXN_DayID_RR() SPLIT RANGE (20190130)
--ALTER PARTITION SCHEME partition_scheme_DBO_TRXN_DayID_RR NEXT USED DBO_TRXN
--ALTER PARTITION FUNCTION partition_function_DBO_TRXN_DayID_RR() SPLIT RANGE (20190131) 


--Begin Tran

--set identity_insert dbo.trxn on 
--Insert into dbo.trxn
--(TransactionId, SourceTransactionId, SourceAccountId, AccountId, FIMerchantId, DayId, Amount, CreateDayID, AuthorizationLocalDT, AuthorizationGMTOffset, AuthorizationAmount, CashBackAmount, CardNetwork, Last4)
--Select TransactionId, SourceTransactionId, SourceAccountId, AccountId, FIMerchantId, DayId, Amount, CreateDayID, AuthorizationLocalDT, AuthorizationGMTOffset, AuthorizationAmount, CashBackAmount, CardNetwork, Last4
--From sqlops.dbo.Trxn_Tmp

--set identity_insert dbo.trxn off 
--commit tran