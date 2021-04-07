create table etl_job_program_action(
job_program_action_cd varchar(16) NOT NULL
,job_program_action_name varchar(64) NULL
,job_program_action_desc varchar(512) NULL
,record_status varchar(1) NULL
,create_by varchar(16) NULL
,create_dt timestamp NULL
,modify_by varchar(16) NULL
,modify_dt timestamp NULL
,org_cd varchar(16) NULL
,loc_cd varchar(16) NULL
,grp_cd varchar(16) NOT NULL
)