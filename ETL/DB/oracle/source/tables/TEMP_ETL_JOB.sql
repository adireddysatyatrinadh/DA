CREATE GLOBAL TEMPORARY TABLE temp_etl_job
(
    job_cd varchar2(128) NOT NULL,
    job_group_cd varchar2(32),
    job_worker_cd varchar2(128) ,
    grp_cd varchar2(64) NOT NULL,
    CONSTRAINT temp_etl_job_pk PRIMARY KEY (job_cd, grp_cd)
)