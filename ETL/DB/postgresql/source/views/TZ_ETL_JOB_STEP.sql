
  CREATE VIEW TZ_ETL_JOB_STEP AS
SELECT	J.JOB_CD , 
	J.JOB_STEP_CD , 
	J.JOB_STEP_NAME , 
	J.JOB_STEP_DESC , 
	J.JOB_STEP_SEQ , 
	J.JOB_PROGRAM_CD , 
	J.JOB_PROGRAM_ACTION , 
	J.JOB_PROGRAM_ARG1 , 
	J.JOB_PROGRAM_ARG2 , 
	J.JOB_PROGRAM_ARG3 , 
	J.JOB_PROGRAM_ARG4 , 
	J.JOB_PROGRAM_ARG5 , 
	J.JOB_PROGRAM_ARG6 , 
	J.JOB_PROGRAM_ARG7 , 
	J.JOB_PROGRAM_ARG8 , 
	J.JOB_PROGRAM_ARG9 , 
	J.JOB_PROGRAM_ARG10 , 
	J.JOB_ON_ERROR , 
	J.LAST_RUN_STATUS , 
	J.LAST_RUN_START_DT + TZ.UTC_minutes * INTERVAL '1 minute' as LAST_RUN_START_DT , 
	J.LAST_RUN_END_DT + TZ.UTC_minutes * INTERVAL '1 minute' as LAST_RUN_END_DT , 
	J.LAST_RUN_DURATION , 
	J.LAST_SUCCESS_RUN_DT + TZ.UTC_minutes * INTERVAL '1 minute' as LAST_SUCCESS_RUN_DT , 
	J.RECORD_STATUS , 
	J.CREATE_BY , 
	J.CREATE_DT + TZ.UTC_minutes * INTERVAL '1 minute' as CREATE_DT , 
	J.MODIFY_BY , 
	J.MODIFY_DT + TZ.UTC_minutes * INTERVAL '1 minute' as MODIFY_DT, 
	J.LOC_CD , 
	J.ORG_CD , 
	J.GRP_CD 
FROM ETL_JOB_STEP J
CROSS JOIN  ETL_TIMEZONE TZ

