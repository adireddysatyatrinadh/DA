CREATE TABLE da_district (
    district_cd varchar2(8) NOT NULL,
    district_name varchar2(64),
    district_desc varchar2(64),
    state_cd varchar2(8),
    country_cd varchar2(8),
    grp_cd varchar2(15) NOT NULL,
    record_status char(1),
    dw_last_updated_dt date,
    dw_facility_cd varchar2(16),
    dw_job_run_no integer,
    dw_row_id varchar2(128)
);

