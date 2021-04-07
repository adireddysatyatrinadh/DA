  CREATE VIEW TZ_ETL_JOB_STATUS AS
SELECT 	J.JOB_STATUS_CD , 
	J.JOB_STATUS_NAME , 
	J.RECORD_STATUS , 
	J.CREATE_BY , 
	J.CREATE_DT + TZ.UTC_minutes/1440 as CREATE_DT, 
	J.MODIFY_BY , 
	J.MODIFY_DT + TZ.UTC_minutes/1440 as MODIFY_DT, 
	J.LOC_CD , 
	J.ORG_CD,  
	J.GRP_CD 
FROM ETL_JOB_STATUS J 
CROSS JOIN  ETL_TIMEZONE TZ