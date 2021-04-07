CREATE TABLE ETL_JOB_SCHEDULE(
	JOB_CD  varchar(256)NOT NULL,
	SCH_CD  varchar(256)NOT NULL,
	JOB_NEXT_RUN_DT timestamp NULL,
	RECORD_STATUS  varchar(1) NULL,
	CREATE_BY  varchar(256)NULL,
	CREATE_DT timestamp NULL,
	MODIFY_BY  varchar(256)NULL,
	MODIFY_DT timestamp NULL,
	LOC_CD  varchar(16)NULL,
	ORG_CD  varchar(16)NULL,
	GRP_CD  varchar(16)NOT NULL

);