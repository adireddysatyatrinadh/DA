CREATE TABLE da_doctor (
    doctor_cd varchar2(8) NOT NULL,
    doctor_name varchar2(60),
    record_status varchar2(1),
    department varchar2(8),
    loc_cd varchar2,
    org_cd varchar2(8) NOT NULL,
    grp_cd varchar2(8) NOT NULL,
    dw_last_updated_dt date,
    dw_facility_cd varchar2(16),
    dw_job_run_no integer,
    dw_row_id varchar2(128)
);
