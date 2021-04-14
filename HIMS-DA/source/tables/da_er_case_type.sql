CREATE TABLE da_er_case_type (
    er_case_type_cd varchar2(15),
    er_case_type_name varchar2(64),
    er_case_type_desc varchar2(128),
    record_status char(1),
    loc_cd varchar2(16),
    org_cd varchar2(8),
    grp_cd varchar2(8),
    dw_facility_cd varchar2(16),
    dw_job_run_no integer,
    dw_last_updated_dt date,
    dw_row_id varchar2(128)
);