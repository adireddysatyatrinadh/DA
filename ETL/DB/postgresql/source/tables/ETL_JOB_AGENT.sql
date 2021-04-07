
CREATE TABLE etl_job_agent
(
    job_agent_cd varchar(256) NOT NULL,
    job_agent_name varchar(256),
    job_agent_desc varchar(256) ,
    record_status varchar(1),
	last_run_no integer,
    last_run_status  varchar(1),
	last_run_start_dt timestamp ,
    last_run_end_dt timestamp ,
    last_run_duration varchar(16),
    next_run_dt timestamp ,
	Sch_Cd varchar(256),
    create_by varchar(256),
    create_dt timestamp ,
    modify_by varchar(256),
    modify_dt timestamp ,
    loc_cd varchar(16),
    org_cd varchar(16),
    grp_cd varchar(16) NOT NULL
  
)