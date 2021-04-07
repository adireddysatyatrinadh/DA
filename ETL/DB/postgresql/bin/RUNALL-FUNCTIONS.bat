

REM "Comment the following parameters if executing only DML"
REM "Uncomment the following parameter if executing as part of "ALL" option"
SET PGHOST=%~1
SET PGUSER=%~2
SET PGPASSWORD=%~3
SET PGDBNAME=%~4
SET PGPORT=%~5
SET PRODUCT_NAME=%~6
SET SOURCE_FOLDER_NAME=%~7


.\BIN\Build -pgbrf .\log\sql\%PRODUCT_NAME%_RUNALL-FUNCTIONS.sql .\%SOURCE_FOLDER_NAME%\FUNCTIONS\

psql --echo-errors -h %PGHOST% -d %PGDBNAME% -U %PGUSER% -p %PGPORT% -f ".\log\sql\%PRODUCT_NAME%_RUNALL-FUNCTIONS.sql" -o ".\log\%PRODUCT_NAME%_RUNALL-FUNCTIONS.log" 2> ".\log\%PRODUCT_NAME%_RUNALL-FUNCTIONS.err 



