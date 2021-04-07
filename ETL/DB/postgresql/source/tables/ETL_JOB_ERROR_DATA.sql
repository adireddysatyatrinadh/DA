create table etl_job_error_data(
job_error_data_id varchar(256)
,last_job_error_log_id varchar(256)
,last_job_step_log_id varchar(256)
,last_job_log_id varchar(256)
,job_step_cd varchar(256)
,job_type_cd varchar(256)
,job_cd varchar(256)
,last_job_run_no varchar(256)
,error_type_cd  varchar(256)
,error_cd  varchar(1024)
,error_message timestamp 
,error_dt varchar(1024)
,error_info1 varchar(1024)
,error_info2 varchar(1024)
,error_info3 varchar(1024)
,job_error_status varchar(256)
,job_error_status_dt timestamp 
,record_status varchar(1)
,create_by varchar(256)
,create_dt timestamp 
,modify_by varchar(256)
,modify_dt timestamp 
,org_cd varchar(16)
,loc_cd varchar(16)
,grp_cd varchar(16)
)
