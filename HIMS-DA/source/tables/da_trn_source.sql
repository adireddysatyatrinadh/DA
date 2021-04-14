CREATE TABLE da_trn_source (
    trn_source_cd varchar2(16),
    trn_source_name varchar2(64),
    trn_source_desc varchar2(128),
    record_status char(1),
    loc_cd varchar2(16),
    org_cd varchar2(8),
    grp_cd varchar2(8),
    dw_facility_cd varchar2(16),
    dw_job_run_no integer,
    dw_last_updated_dt date,
    dw_row_id varchar2(128)
);

