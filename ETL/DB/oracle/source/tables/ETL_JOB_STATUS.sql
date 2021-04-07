CREATE TABLE etl_job_status
(
    job_status_cd  varchar2(64) NOT NULL,
    job_status_name  varchar2(64),
    record_status  varchar2(1) ,
    create_by  varchar2(1646) ,
    create_dt  timestamp ,
    modify_by  varchar2(64) ,
    modify_dt  timestamp ,
    loc_cd  varchar2(16) ,
    org_cd  varchar2(16),
    grp_cd  varchar2(16)  NOT NULL
)
