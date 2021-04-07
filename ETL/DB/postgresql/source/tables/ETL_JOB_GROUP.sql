CREATE TABLE etl_JOB_GROUP(
job_group_cd varchar(256)NOT NULL
,job_group_name varchar(256)
,job_group_desc varchar(512)
,record_status varchar(1)
,create_by varchar(256)
,create_dt timestamp
,modify_by varchar(256)
,modify_dt timestamp
,org_cd varchar(16)
,loc_cd varchar(16)
,grp_cd varchar(16)
)