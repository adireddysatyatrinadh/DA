CREATE TABLE ETL_JOB_PROGRAM(
	JOB_PROGRAM_CD  varchar2(64) NOT NULL,
	JOB_PROGRAM_NAME  varchar2(64) NULL,
	JOB_PROGRAM_DESC  varchar2(512) NULL,
	RECORD_STATUS  varchar2(1) NULL,
	CREATE_BY  varchar2(16) NULL,
	CREATE_DT timestamp NULL,
	MODIFY_BY  varchar2(16) NULL,
	MODIFY_DT timestamp NULL,
	ORG_CD  varchar2(16) NULL,
	LOC_CD  varchar2(16) NULL,
	GRP_CD  varchar2(16) NOT NULL

);