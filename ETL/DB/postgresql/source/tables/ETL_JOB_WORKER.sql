create table etl_job_worker(
job_worker_cd varchar(256) not null
,job_worker_name varchar(256) 
,job_worker_desc varchar(512)
,record_status varchar(1)
,job_agent_cd varchar(256) not null
,last_run_no integer
,last_run_status varchar(256) 
,last_run_start_dt timestamp 
,last_run_end_dt timestamp 
,last_run_duration varchar(16) 
,next_run_dt timestamp 
,SCH_CD varchar(256) 
,create_by varchar(256) 
,create_dt timestamp 
,modify_by varchar(256) 
,modify_dt timestamp 
,loc_cd varchar(16)
,org_cd varchar(16) 
,grp_cd varchar(16) not null)