CREATE TABLE etl_job
(
    job_cd varchar2(64) NOT NULL,
    job_name varchar2(64),
    job_desc varchar2(512),
    job_group_cd varchar2(32),
    last_run_no integer,
    last_run_status varchar2(1),
    last_run_start_dt timestamp,
    last_run_end_dt timestamp,
    last_run_duration varchar2(16),
    last_success_run_dt timestamp,
    next_run_dt timestamp,
	job_worker_cd  varchar2(32),
	record_status varchar2(1),
    create_by varchar2(32),
    create_dt timestamp,
    modify_by varchar2(32),
    modify_dt timestamp,
    LOC_cd varchar2(16),
    org_cd varchar2(16),
    grp_cd varchar2(16) NOT NULL
  
)

