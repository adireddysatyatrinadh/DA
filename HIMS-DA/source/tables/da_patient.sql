CREATE TABLE da_patient (
    umr_no varchar2(15) NOT NULL,
    patient_name varchar2(96),
    gender_cd char(1),
    dob date,
    age varchar2(4000),
    reg_no varchar2(15),
    reg_dt date,
    area_cd varchar2(8),
    city_cd varchar2(8),
    district_cd varchar2(8),
    state_cd varchar2(8),
    country_cd varchar2(8),
    loc_cd varchar2(15),
    org_cd varchar2(8) NOT NULL,
    grp_cd varchar2(8) NOT NULL,
    record_status char(1),
    dw_last_updated_dt date,
    dw_facility_cd varchar2(16),
    dw_job_run_no integer,
    dw_row_id varchar2(128)
);

