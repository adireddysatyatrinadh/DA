
CREATE TABLE etl_job_step_log
(job_step_log_id integer not null
,job_log_id integer
,job_step_cd varchar(256)
,job_cd  varchar(256) NOT NULL
,job_run_no integer
,run_status varchar(1)
,run_start_dt  timestamp   NOT NULL
,run_end_dt  timestamp 
,run_duration varchar(16)
,job_step_output varchar(1000)
,job_step_error_message varchar(1000)
,record_status varchar(1)
,create_by varchar(256)
,create_dt  timestamp 
,modify_by varchar(256)
,modify_dt  timestamp 
,loc_cd varchar(16)
,org_cd varchar(16)
,grp_cd varchar(16)  NOT NULL
)