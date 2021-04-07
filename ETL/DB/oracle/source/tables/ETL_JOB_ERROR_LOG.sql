create table etl_job_error_log
(job_error_log_id varchar2(32)
,job_step_log_id varchar2(32)
,job_log_id varchar2(16)
,job_step_cd varchar2(64)
,job_group_cd varchar2(64)
,job_cd varchar2(64)
,job_run_no integer NOT NULL
,error_source varchar2(256)
,error_type_cd varchar2(64)
,error_cd varchar2(64)
,error_message varchar2(1024)
,error_dt timestamp 
,error_info1 varchar2(1024)
,error_info2 varchar2(1024)
,error_info3 varchar2(1024)
,record_status varchar2(1)
,create_by varchar2(32)
,create_dt timestamp 
,modify_by varchar2(32)
,modify_dt  timestamp 
,org_cd varchar2(16)
,loc_cd varchar2(16)
,grp_cd varchar2(16)
)
