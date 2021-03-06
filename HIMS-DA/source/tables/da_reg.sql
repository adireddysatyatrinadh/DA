
CREATE TABLE da_reg (
    reg_no varchar2(15) NOT NULL,
    reg_dt date,
    umr_no varchar2(15),
    area_cd varchar2(8),
    city_cd varchar2(8),
    district_cd varchar2(8),
    state_cd varchar2(8),
    country_cd varchar2(8),
    referal_source_cd char(1),
    nationality_cd varchar2(32),
    expiry_dt date,
    is_expired char(1),
    trn_source_cd varchar2,
    company_cd varchar2(8),
    company_type_cd varchar2(15),
    rec_type_cd varchar2(1),
    record_status char(1),
    loc_cd varchar2(15),
    org_cd varchar2(8) NOT NULL,
    grp_cd varchar2(8) NOT NULL,
    dw_last_updated_dt date,
    dw_facility_cd varchar2(16),
    dw_job_run_no integer,
    dw_row_id varchar2(128),
    age varchar2(16)
);
