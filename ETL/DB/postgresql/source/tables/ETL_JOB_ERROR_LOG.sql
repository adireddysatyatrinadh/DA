create table etl_job_error_log
(job_error_log_id varchar(256)
,job_step_log_id varchar(256)
,job_log_id varchar(256)
,job_step_cd varchar(256)
,job_group_cd varchar(256)
,job_cd varchar(256)
,job_run_no integer NOT NULL
,error_source varchar(256)
,error_type_cd varchar(256)
,error_cd varchar(256)
,error_message varchar(1024)
,error_dt timestamp 
,error_info1 varchar(1024)
,error_info2 varchar(1024)
,error_info3 varchar(1024)
,record_status varchar(1)
,create_by varchar(256)
,create_dt timestamp 
,modify_by varchar(256)
,modify_dt  timestamp 
,org_cd varchar(16)
,loc_cd varchar(16)
,grp_cd varchar(16)
)
