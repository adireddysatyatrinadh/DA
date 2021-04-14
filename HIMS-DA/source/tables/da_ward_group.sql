CREATE TABLE da_ward_group (
    ward_group_cd varchar2(8) NOT NULL,
    ward_group_name varchar2(32),
    ward_category_cd varchar2,
    record_status char(1),
    loc_cd varchar2,
    org_cd varchar2(8) NOT NULL,
    grp_cd varchar2(8) NOT NULL,
    dw_last_updated_dt date,
    dw_facility_cd varchar2(16),
    dw_job_run_no integer,
    dw_row_id varchar2(128)
);

