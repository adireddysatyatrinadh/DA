CREATE TABLE ETL_JOB_SCHEDULE(
	JOB_CD  varchar2(64) NOT NULL,
	SCH_CD  varchar2(64) NOT NULL,
	JOB_NEXT_RUN_DT timestamp NULL,
	RECORD_STATUS  varchar2(1) NULL,
	CREATE_BY  varchar2(64) NULL,
	CREATE_DT timestamp NULL,
	MODIFY_BY  varchar2(64) NULL,
	MODIFY_DT timestamp NULL,
	LOC_CD  varchar2(16) NULL,
	ORG_CD  varchar2(16) NULL,
	GRP_CD  varchar2(16) NOT NULL

);