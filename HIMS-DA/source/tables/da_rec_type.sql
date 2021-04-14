CREATE TABLE da_rec_type (
    rec_type_cd char(1),
    rec_type_name char(5),
    rec_type_desc char(5),
    record_status char(1),
    loc_cd varchar2,
    org_cd varchar2(8) NOT NULL,
    grp_cd varchar2(8) NOT NULL,
    dw_last_updated_dt date,
    dw_facility_cd varchar2(16),
    dw_job_run_no integer,
    dw_row_id varchar2(128)
);