CREATE TABLE da_area (
    area_cd varchar2(8) NOT NULL,
    area_name varchar2(64),
    area_desc varchar2(64),
    city_cd varchar2(8),
    district_cd varchar2(8),
    state_cd varchar2(8),
    country_cd varchar2(8),
    grp_cd varchar2(15) NOT NULL,
    record_status CHAR(1),
    dw_last_updated_dt date,
    dw_facility_cd varchar2(16),
    dw_row_id varchar2(128),
    dw_job_run_no integer
);

