
CREATE TABLE etl_job_error_status
(
    job_error_status_cd varchar(256),
    job_error_status_name varchar(256),
    record_status varchar(1) ,
    create_by varchar(256),
    create_dt timestamp  ,
    modify_by varchar(256),
    modify_dt timestamp  ,
    org_cd varchar(16),
    loc_cd varchar(16),
    grp_cd varchar(16)
)