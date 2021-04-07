
CREATE TABLE etl_job_step_log
(job_step_log_id integer not null
,job_log_id integer
,job_step_cd varchar2(64)
,job_cd  varchar2(64)  NOT NULL
,job_run_no integer
,run_status varchar2(1)
,run_start_dt  timestamp   NOT NULL
,run_end_dt  timestamp 
,run_duration varchar2(16)
,job_step_output varchar2(1000)
,job_step_error_message varchar2(1000)
,record_status varchar2(1)
,create_by varchar2(32)
,create_dt  timestamp 
,modify_by varchar2(32)
,modify_dt  timestamp 
,loc_cd varchar2(16)
,org_cd varchar2(16)
,grp_cd varchar2(16)  NOT NULL
)