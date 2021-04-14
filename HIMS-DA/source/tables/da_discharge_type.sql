CREATE TABLE da_discharge_type (
    discharge_type_cd varchar2(32) NOT NULL,
    discharge_type_desc varchar2(32),
    record_status char(1),
    grp_cd varchar2(8) NOT NULL,
    dw_last_updated_dt date,
    dw_facility_cd varchar2(16),
    dw_job_run_no integer,
    dw_row_id varchar2(128)
);

