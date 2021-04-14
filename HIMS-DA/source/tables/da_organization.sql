
CREATE TABLE da_organization (
    org_cd varchar2(8) NOT NULL,
    org_name varchar2(64),
    org_desc varchar2(64),
    grp_cd varchar2(8) NOT NULL,
    record_status char(1),
    dw_last_updated_dt date,
    dw_facility_cd varchar2(16),
    dw_job_run_no integer,
    dw_row_id varchar2(128)
);

