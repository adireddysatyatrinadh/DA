create table etl_job_program_action(
job_program_action_cd varchar2(64) NOT NULL
,job_program_action_name varchar2(64) NULL
,job_program_action_desc varchar2(512) NULL
,record_status varchar2(1) NULL
,create_by varchar2(16) NULL
,create_dt timestamp NULL
,modify_by varchar2(16) NULL
,modify_dt timestamp NULL
,org_cd varchar2(16) NULL
,loc_cd varchar2(16) NULL
,grp_cd varchar2(16) NOT NULL
)