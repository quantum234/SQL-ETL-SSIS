sp_configure 'clr enabled', 1
GO
RECONFIGURE
GO


CREATE ASSEMBLY 
--assembly name for references from SQL script
SqlRegularExpressions 
-- assembly name and full path to assembly dll, SqlRegularExpressions in this case
from 'J:\SqlRegularExpressions\bin\Release\SqlRegularExpressions.dll' 
WITH PERMISSION_SET = SAFE



CREATE FUNCTION RegExpLike(@Text nvarchar(max), @Pattern nvarchar(255)) RETURNS BIT
--function external name
AS EXTERNAL NAME SqlRegularExpressions.SqlRegularExpressions.[Like]


CREATE FUNCTION 
--signature of SQL Tabled function
RegExpMatches(@text nvarchar(max), @pattern nvarchar(255))
RETURNS TABLE 
([Index] int, [Length] int, [Value] nvarchar(255))
AS 
--external name of method wich returns matches
EXTERNAL NAME SqlRegularExpressions.SqlRegularExpressions.GetMatches
GO


select * from [Database].[dbo].[Table]
where 1 = dbo.RegExpLike(name, '\s\d*\D\d*\D\d*\D\d*\D\d*\D\d*\s\d')