CREATE TABLE da_admission_type (
    admission_type_cd varchar2,
    admission_type_name varchar2,
    record_status char(1),
    loc_cd varchar2(16),
    org_cd varchar2(8),
    grp_cd varchar2(8),
    dw_last_updated_dt DATE,
    dw_facility_cd varchar2(16),
    dw_job_run_no INTEGER,
    dw_row_id varchar2(128)
);
