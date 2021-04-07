   CREATE  VIEW TZ_ETL_JOB_WORKER_JOBS AS
SELECT 	J.JOB_WORKER_CD, 
	J.RECORD_STATUS , 
	J.JOB_CD , 
	J.JOB_GROUP_CD , 
	J.CREATE_BY , 
	J.CREATE_DT + TZ.UTC_minutes * INTERVAL '1 minute' as CREATE_DT, 
	J.MODIFY_BY , 
	J.MODIFY_DT + TZ.UTC_minutes * INTERVAL '1 minute' as MODIFY_DT, 
	J.LOC_CD , 
	J.ORG_CD , 
	J.GRP_CD 

FROM ETL_JOB_WORKER_JOBS J
CROSS JOIN  ETL_TIMEZONE TZ
