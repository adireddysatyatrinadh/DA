create table etl_job_worker_jobs(
job_worker_cd varchar2(64) not null
,record_status varchar2(1)
,job_cd varchar2(64) not null
,job_group_cd varchar2(64)
,create_by varchar2(64)
,create_dt timestamp 
,modify_by varchar2(64)
,modify_dt timestamp 
,loc_cd varchar2(16)
,org_cd varchar2(16)
,grp_cd varchar2(16) not null
)