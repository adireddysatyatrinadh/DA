create table etl_job_worker_jobs(
job_worker_cd varchar(256) not null
,record_status varchar(1)
,job_cd varchar(256) not null
,job_group_cd varchar(256) 
,create_by varchar(256) 
,create_dt timestamp 
,modify_by varchar(256) 
,modify_dt timestamp 
,loc_cd varchar(16)
,org_cd varchar(16)
,grp_cd varchar(16) not null
)