
CREATE TABLE da_receipt_reference_type (
    receipt_reference_type_cd varchar2(32),
    receipt_reference_type_name varchar2(64),
    receipt_reference_type_desc varchar2(128),
    record_status char(1),
    loc_cd varchar2(16),
    org_cd varchar2(8),
    grp_cd varchar2(8),
    dw_last_updated_dt date,
    dw_facility_cd varchar2(16),
    dw_job_run_no integer,
    dw_row_id varchar2(128)
);