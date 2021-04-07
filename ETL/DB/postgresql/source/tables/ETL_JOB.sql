CREATE TABLE ETL_job
(
    job_cd varchar(256) NOT NULL,
    job_name varchar(256),
    job_desc varchar(256),
    job_group_cd varchar(256),
    last_run_no integer,
    last_run_status varchar(1),
    last_run_start_dt timestamp,
    last_run_end_dt timestamp,
    last_run_duration varchar(16),
    last_success_run_dt timestamp,
    next_run_dt timestamp,
    job_worker_cd  varchar(256),
    record_status varchar(1),
    create_by varchar(256),
    create_dt timestamp,
    modify_by varchar(256),
    modify_dt timestamp,
    LOC_cd varchar(16),
    org_cd varchar(16),
    grp_cd varchar(16) NOT NULL
  
)

