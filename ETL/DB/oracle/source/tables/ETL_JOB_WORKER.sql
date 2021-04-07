create table etl_job_worker(
job_worker_cd varchar2(64) not null
,job_worker_name varchar2(64)
,job_worker_desc varchar2(512)
,record_status varchar2(1)
,job_agent_cd varchar2(64) not null
,last_run_no integer
,last_run_status varchar2(16)
,last_run_start_dt timestamp 
,last_run_end_dt timestamp 
,last_run_duration varchar2(16)
,next_run_dt timestamp 
,SCH_CD varchar2(64)
,create_by varchar2(1664)
,create_dt timestamp 
,modify_by varchar2(64)
,modify_dt timestamp 
,loc_cd varchar2(16)
,org_cd varchar2(16) 
,grp_cd varchar2(16) not null)