CREATE GLOBAL TEMPORARY TABLE temp_etl_job_worker_jobs
(
    job_cd varchar(64) NOT NULL,
    job_worker_cd varchar(64) ,
    grp_cd varchar(64) NOT NULL,
    CONSTRAINT temp_etl_job_worker_jobs_pk PRIMARY KEY (job_worker_cd,job_cd, grp_cd)
)