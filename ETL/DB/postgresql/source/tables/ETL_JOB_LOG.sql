CREATE TABLE etl_job_log
(job_log_id  varchar(256)
,job_cd  varchar(256)not null
,job_run_no  integer
,run_status  varchar(1)
,run_start_dt  timestamp  
,run_end_dt  timestamp 
,run_duration   varchar(16)
,record_status  varchar(1)
,create_by  varchar(256)
,create_dt  timestamp 
,modify_by  varchar(256)
,modify_dt  timestamp 
,loc_cd  varchar(16)
,org_cd  varchar(16)
,grp_cd  varchar(16)not null
 )

