CREATE TABLE da_city (
    city_cd varchar2(8) NOT NULL,
    city_name varchar2(64),
    city_desc varchar2(64),
    district_cd varchar2(8),
    state_cd varchar2(8),
    country_cd varchar2(8),
    grp_cd varchar2(8) NOT NULL,
    record_status char(1),
    dw_last_updated_dt date,
    dw_facility_cd varchar2(16),
    dw_job_run_no integer,
    dw_row_id varchar2(128)
);
