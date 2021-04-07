
CREATE TABLE etl_job_error_status
(
    job_error_status_cd varchar2(64) ,
    job_error_status_name varchar2(64) ,
    record_status varchar2(1) ,
    create_by varchar2(64) ,
    create_dt timestamp  ,
    modify_by varchar2(64) ,
    modify_dt timestamp  ,
    org_cd varchar2(16) ,
    loc_cd varchar2(16) ,
    grp_cd varchar2(16) 
)