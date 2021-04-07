
  CREATE VIEW TZ_ETL_JOB_ERROR_DATA AS 
   SELECT J.JOB_ERROR_DATA_ID , 
	J.LAST_JOB_ERROR_LOG_ID, 
	J.LAST_JOB_STEP_LOG_ID , 
	J.LAST_JOB_LOG_ID , 
	J.JOB_STEP_CD , 
	J.JOB_TYPE_CD , 
	J.JOB_CD , 
	J.LAST_JOB_RUN_NO , 
	J.ERROR_TYPE_CD , 
	J.ERROR_CD , 
	J.ERROR_MESSAGE + TZ.UTC_minutes/1440 as ERROR_MESSAGE, 
	J.ERROR_DT , 
	J.ERROR_INFO1 , 
	J.ERROR_INFO2 , 
	J.ERROR_INFO3 , 
	J.JOB_ERROR_STATUS , 
	J.JOB_ERROR_STATUS_DT + TZ.UTC_minutes/1440 as JOB_ERROR_STATUS_DT, 
	J.RECORD_STATUS , 
	J.CREATE_BY , 
	J.CREATE_DT + TZ.UTC_minutes/1440 as CREATE_DT, 
	J.MODIFY_BY , 
	J.MODIFY_DT + TZ.UTC_minutes/1440 as MODIFY_DT, 
	J.ORG_CD , 
	J.LOC_CD , 
	J.GRP_CD 
 	FROM ETL_JOB_ERROR_DATA J
	CROSS JOIN  ETL_TIMEZONE TZ	

