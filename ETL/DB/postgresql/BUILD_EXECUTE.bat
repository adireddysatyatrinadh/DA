REM ***********************************************************************************************************
REM "IF DONT WANT TO EXECUTE ANY BATCH FILE THEN PREFIX BATCH FILE CALL WITH <REM> "

REM Modify the following variables as per requirement


SET HOST="RNDSERVER.SUVARNA01.LOCAL"
SET USER_NAME=postgres
SET PASSWORD=p@ssw0rd
SET DB_NAME="SUV_R_INTK_M"
SET PORT=5433


REM ***********************************************************************************************************

IF NOT EXIST ".\log" MKDIR .\log
IF NOT EXIST ".\log\sql" MKDIR .\log\sql

TYPE NUL > .\log\BUILD_EXECUTE.log

ECHO BEGIN::DATETIME::BUILD_EXECUTE: %date%-%time% >> .\log\BUILD_EXECUTE.log

ECHO BEGIN::DATETIME::RUNALL-OBJECTS-INTALK: %date%-%time% >> .\log\BUILD_EXECUTE.log
SET PRODUCT_NAME=INTALK
SET SOURCE_FOLDER_NAME=SOURCE
CALL .\BIN\RUNALL-OBJECTS-INTALK %HOST% %USER_NAME% %PASSWORD% %DB_NAME% %PORT% %PRODUCT_NAME% %SOURCE_FOLDER_NAME%>> .\log\BUILD_EXECUTE.log
ECHO END::DATETIME::RUNALL-OBJECTS-INTALK: %date%-%time% >> .\log\BUILD_EXECUTE.log

ECHO END::DATETIME::BUILD_EXECUTE: %date%-%time% >> .\log\BUILD_EXECUTE.log
ECHO OFF

find /I "::DATETIME::" .\log\BUILD_EXECUTE.log
find /I "::DATETIME::" .\log\BUILD_EXECUTE.log >> .\log\BUILD_EXECUTE.log

ECHO Build Execution Completed, Press any key to continue...
PAUSE > NUL