
CREATE TABLE etl_job_agent
(
    job_agent_cd varchar2(64) NOT NULL,
    job_agent_name varchar2(64),
    job_agent_desc varchar2(512) ,
    record_status varchar2(1),
	last_run_no integer,
    last_run_status  varchar2(1),
	last_run_start_dt timestamp ,
    last_run_end_dt timestamp ,
    last_run_duration varchar2(16),
    next_run_dt timestamp ,
	Sch_Cd varchar2(64),
    create_by varchar2(64) ,
    create_dt timestamp ,
    modify_by varchar2(64) ,
    modify_dt timestamp ,
    loc_cd varchar2(16)  ,
    org_cd varchar2(16)  ,
    grp_cd varchar2(16)   NOT NULL
  
)