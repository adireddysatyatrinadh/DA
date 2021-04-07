create table etl_job_error_data(
job_error_data_id varchar2(16)
,last_job_error_log_id varchar2(16)
,last_job_step_log_id varchar2(16)
,last_job_log_id varchar2(16)
,job_step_cd varchar2(64)
,job_type_cd varchar2(64)
,job_cd varchar2(64)
,last_job_run_no varchar2(32)
,error_type_cd  varchar2(64)
,error_cd  varchar2(1024)
,error_message timestamp 
,error_dt varchar2(1024)
,error_info1 varchar2(1024)
,error_info2 varchar2(1024)
,error_info3 varchar2(1024)
,job_error_status varchar2(16)
,job_error_status_dt timestamp 
,record_status varchar2(1)
,create_by varchar2(64)
,create_dt timestamp 
,modify_by varchar2(64)
,modify_dt timestamp 
,org_cd varchar2(16)
,loc_cd varchar2(16)
,grp_cd varchar2(16)
)
