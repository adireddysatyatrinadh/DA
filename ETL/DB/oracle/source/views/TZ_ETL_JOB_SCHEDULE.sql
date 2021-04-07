
  CREATE VIEW TZ_ETL_JOB_SCHEDULE AS
SELECT	J.JOB_CD , 
	J.SCH_CD, 
	J.JOB_NEXT_RUN_DT + TZ.UTC_minutes/1440 as JOB_NEXT_RUN_DT, 
	J.RECORD_STATUS , 
	J.CREATE_BY , 
	J.CREATE_DT + TZ.UTC_minutes/1440 as CREATE_DT, 
	J.MODIFY_BY , 
	J.MODIFY_DT + TZ.UTC_minutes/1440 as MODIFY_DT, 
	J.LOC_CD , 
	J.ORG_CD , 
	J.GRP_CD 
FROM ETL_JOB_SCHEDULE J
CROSS JOIN  ETL_TIMEZONE TZ
