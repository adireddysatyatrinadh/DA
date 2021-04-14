CREATE TABLE da_referal_type (
    referal_type_cd varchar2(32) NOT NULL,
    referal_type_name varchar2(32),
    referal_type_desc varchar2(32),
    record_status char(1),
    loc_cd varchar2,
    org_cd varchar2(8) NOT NULL,
    grp_cd varchar2(8) NOT NULL,
    dw_facility_cd varchar2(16),
    dw_last_updated_dt date,
    dw_job_run_no integer,
    dw_row_id varchar2(128)
);

