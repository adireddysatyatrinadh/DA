CREATE TABLE da_ward (
    ward_cd varchar2(8),
    ward_name varchar2(32),
    ward_group_cd varchar2(8),
    record_status char(1),
    loc_cd varchar2(16),
    org_cd varchar2(8),
    grp_cd varchar2(8),
    dw_facility_cd varchar2(16),
    dw_job_run_no integer,
    dw_last_updated_dt date,
    dw_row_id varchar2(128)
);

