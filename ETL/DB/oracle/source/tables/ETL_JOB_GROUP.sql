CREATE TABLE etl_job_group(
job_group_cd varchar2(64) NOT NULL
,job_group_name varchar2(64)
,job_group_desc varchar2(512)
,record_status varchar2(1)
,create_by varchar2(16)
,create_dt timestamp
,modify_by varchar2(16)
,modify_dt timestamp
,org_cd varchar2(16)
,loc_cd varchar2(16)
,grp_cd varchar2(16)
)