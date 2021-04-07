CREATE VIEW TZ_ETL_JOB_LOG AS
  SELECT J.JOB_LOG_ID , 
	J.JOB_CD , 
	J.JOB_RUN_NO , 
	J.RUN_STATUS , 
	J.RUN_START_DT + TZ.UTC_minutes/1440 as RUN_START_DT , 
	J.RUN_END_DT + TZ.UTC_minutes/1440 as RUN_END_DT , 
	J.RUN_DURATION , 
	J.RECORD_STATUS , 
	J.CREATE_BY , 
	J.CREATE_DT + TZ.UTC_minutes/1440 as CREATE_DT , 
	J.MODIFY_BY , 
	J.MODIFY_DT + TZ.UTC_minutes/1440 as MODIFY_DT , 
	J.LOC_CD , 
	J.ORG_CD , 
	J.GRP_CD 
 FROM ETL_JOB_LOG J
 CROSS JOIN  ETL_TIMEZONE TZ 
 
 
