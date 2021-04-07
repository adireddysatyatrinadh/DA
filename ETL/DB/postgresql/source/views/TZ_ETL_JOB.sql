  CREATE or replace VIEW TZ_ETL_JOB  AS 
  SELECT J.JOB_CD, 
	J.JOB_NAME, 
	J.JOB_DESC, 
	J.JOB_GROUP_CD, 
	J.LAST_RUN_NO, 
	J.LAST_RUN_STATUS,
	J.LAST_RUN_START_DT + TZ.UTC_minutes * INTERVAL '1 minute' as LAST_RUN_START_DT, 
	J.LAST_RUN_END_DT + TZ.UTC_minutes* INTERVAL '1 minute' as LAST_RUN_END_DT, 
	J.LAST_RUN_DURATION , 
	J.LAST_SUCCESS_RUN_DT + TZ.UTC_minutes* INTERVAL '1 minute' as LAST_SUCCESS_RUN_DT, 
	J.NEXT_RUN_DT + TZ.UTC_minutes* INTERVAL '1 minute' as NEXT_RUN_DT,
	J.JOB_WORKER_CD , 
	J.RECORD_STATUS , 
	J.CREATE_BY , 
	J.CREATE_DT + TZ.UTC_minutes* INTERVAL '1 minute' as CREATE_DT, 
	J.MODIFY_BY , 
	J.MODIFY_DT + TZ.UTC_minutes* INTERVAL '1 minute' as MODIFY_DT, 
	J.LOC_CD, 
	J.ORG_CD, 
	J.GRP_CD
   FROM ETL_JOB as J
   CROSS JOIN  ETL_TIMEZONE as TZ;
 
 


