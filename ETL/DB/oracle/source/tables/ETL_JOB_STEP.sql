CREATE TABLE etl_job_step
(
    job_cd  varchar2(64) NOT NULL,
    job_step_cd  varchar2(64) NOT NULL,
    job_step_name  varchar2(64),
    job_step_desc  varchar2(512),
    job_step_seq integer,
    job_program_cd  varchar2(64) ,
    job_program_action  varchar2(1000),
	Job_program_arg1  varchar2(1000),
    Job_program_arg2  varchar2(1000),
	Job_program_arg3  varchar2(1000),
	Job_program_arg4  varchar2(1000),
	Job_program_arg5  varchar2(1000),
	Job_program_arg6  varchar2(1000),
	Job_program_arg7  varchar2(1000),
	Job_program_arg8  varchar2(1000),
	Job_program_arg9  varchar2(1000),
	Job_program_arg10  varchar2(1000),
	job_on_error  varchar2(64),
    last_run_status  varchar2(16) ,
    last_run_start_dt  timestamp ,
    last_run_end_dt  timestamp ,
    last_run_duration varchar2(16) ,
    last_success_run_dt  timestamp ,
    record_status  varchar2(1) ,
    create_by  varchar2(32),
    create_dt  timestamp ,
    modify_by  varchar2(32),
    modify_dt  timestamp ,
    loc_cd  varchar2(16),
    org_cd  varchar2(16),
    grp_cd  varchar2(16)  NOT NULL
   
)