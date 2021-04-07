CREATE GLOBAL TEMPORARY TABLE temp_etl_job
(
    job_cd varchar(128) NOT NULL,
    job_group_cd varchar(32),
    job_worker_cd varchar(128) ,
    grp_cd varchar(64) NOT NULL,
    CONSTRAINT temp_etl_job_pk PRIMARY KEY (job_cd, grp_cd)
)