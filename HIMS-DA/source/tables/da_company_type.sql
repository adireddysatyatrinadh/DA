CREATE TABLE da_company_type (
    company_type_cd varchar2(15) NOT NULL,
    company_type_name varchar2(64),
    record_status char(1),
    loc_cd varchar2,
    org_cd varchar2(15) NOT NULL,
    grp_cd varchar2(15) NOT NULL,
    dw_last_updated_dt date,
    dw_facility_cd varchar2(16),
    dw_job_run_no integer,
    dw_row_id varchar2(128)
);

