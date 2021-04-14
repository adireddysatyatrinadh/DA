CREATE TABLE da_department (
    department_cd varchar2(8) NOT NULL,
    department_desc varchar2(64),
    department_name varchar2(64),
    depttype char(1),
    record_status char(1),
    loc_cd varchar2(15),
    org_cd varchar2(8) NOT NULL,
    grp_cd varchar2(8) NOT NULL,
    dw_last_updated_dt date,
    dw_facility_cd varchar2(16),
    dw_job_run_no integer,
    dw_row_id varchar2(128)
);

