
CREATE GLOBAL TEMPORARY TABLE temp_etl_worker_load
(
    job_worker_cd varchar(128) ,
    job_cnt integer,
    CONSTRAINT temp_etl_worker_load_pk PRIMARY KEY (job_worker_cd)
)
