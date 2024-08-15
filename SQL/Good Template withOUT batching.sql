 
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
/*
==============================================================================================================================================

Name:			
Author:			
Create date:	
Description:	This script will remove all duplicates from the XXXXXX table grouped by XXXXX and XXXXX. 

				Script logs number of records to be removed and number removed but Production Support will also verify after running script
				
				Because this script will only remove less than 150 records I will not be using batching with this script
==============================================================================================================================================
*/

USE OPSBATCH
GO

-- Standard Variables --
DECLARE @rowCt INT				
		, @procStartDt DATETIME = GETDATE()
		, @startDt DATETIME = GETDATE()
		, @subProgram INT = @@PROCID
		, @msg VARCHAR(4000) = ''		
		, @programLocator SMALLINT = 0
		, @errorNumber INT = 0
		, @errorLine INT = 0
		, @errorMessage NVARCHAR(2048) = N''
		, @errorProcedure NVARCHAR(126) = N''
		, @isDebugMsg INT = 0
		, @severity VARCHAR(2)

-- Additional Variables --
		, @countRecs BIGINT
		, @script VARCHAR(350) = ''GETDATE()_'DuplicateRemoval'
		, @appId int = 999
;

BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from interfering with SELECT statements.
	SET NOCOUNT ON;

	

	SET @startDt = GETDATE();

	BEGIN TRY	
	

	-- Getting a count of the records to be deleted.
		SELECT @countRecs = COUNT_BIG(*) from (
		select 
		ROW_NUMBER() over(partition by XXXXX, XXXXX order by CreateDt asc) as ttl
		FROM [SCHEMA].[TABLENAME] with(nolock)
		)x where ttl > 1;
		

	
		BEGIN TRAN;
			
			 -- ;with cte AS
			 -- (
			 -- select *,
				--row_number() over (partition by XXXXX, XXXXX order by CreateDt asc) as ttl
			 -- from [SCHEMA].[TABLENAME] with(nolock)
			 -- )
			 -- delete cte where ttl > 1 
			

			SET @rowCt = @@ROWCOUNT;


				
				
			
		COMMIT TRAN;
	END TRY
	BEGIN CATCH
	
	SELECT @errorProcedure = ISNULL(ERROR_PROCEDURE(), ''), @errorLine=ISNULL(ERROR_LINE(),-1), @errorNumber=ISNULL(ERROR_NUMBER(),-1), @errorMessage=ISNULL(ERROR_MESSAGE(),N'');
		
	IF XACT_STATE() = -1 or @@trancount > 0
		ROLLBACK TRAN;
				

	
END;
GO