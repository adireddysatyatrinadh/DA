CREATE TABLE etl_job_log
(job_log_id  varchar2(32)
,job_cd  varchar2(64) not null
,job_run_no  integer
,run_status  varchar2(1)
,run_start_dt  timestamp  
,run_end_dt  timestamp 
,run_duration   varchar2(16)
,record_status  varchar2(1)
,create_by  varchar2(32)
,create_dt  timestamp 
,modify_by  varchar2(32)
,modify_dt  timestamp 
,loc_cd  varchar2(16)
,org_cd  varchar2(16)
,grp_cd  varchar2(16) not null
 )

