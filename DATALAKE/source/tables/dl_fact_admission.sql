CREATE TABLE public.dl_fact_admission (
    dw_row_id character varying(128),
    admn_no character varying(15),
    admn_dt timestamp without time zone,
    umr_no character varying(15),
    er_no character varying(15),
    er_dt timestamp without time zone,
    trn_source_cd character varying,
    trn_source_name character varying(64),
    er_case_type_cd character varying,
    er_case_type_name character varying(64),
    patient_name character varying(96),
    gender_cd character(1),
    gender_name character varying(32),
    dob timestamp without time zone,
    age character varying(4000),
    reg_no character varying(15),
    reg_dt timestamp without time zone,
    area_cd character varying(8),
    area_name character varying(64),
    city_cd character varying(8),
    city_name character varying(64),
    district_cd character varying(8),
    district_name character varying(64),
    state_cd character varying(8),
    state_name character varying(64),
    country_cd character varying(8),
    country_name character varying(64),
    company_cd character varying(8),
    company_name character varying(60),
    company_type_cd character varying(15),
    company_type_name character varying(64),
    admission_type_cd character varying(8),
    admission_type_name character varying,
    department_cd character varying(8),
    department_name character varying(64),
    doctor_cd character varying(8),
    doctor_name character varying(60),
    ward_cd character varying(8),
    ward_name character varying(32),
    room_cd character varying(15),
    room_name character varying(64),
    rec_type_cd character varying(1),
    rec_type_name character(5),
    loc_cd character varying(64),
    loc_name character varying(128),
    loc_short_name character varying(16),
    org_cd character varying(8),
    org_name character varying(128),
    org_short_name character varying(16),
    grp_cd character varying(8),
    grp_name character varying(64),
    grp_short_name character varying(16),
    age_in_years integer,
    age_in_months integer,
    age_in_days integer,
    record_status character(1),
    dw_facility_cd character varying(16),
    dw_job_run_no numeric,
    dw_last_updated_dt timestamp without time zone
);
