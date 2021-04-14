CREATE TABLE public.da_patient (
    umr_no character varying(15) NOT NULL,
    patient_name character varying(96),
    gender_cd character(1),
    dob timestamp without time zone,
    age character varying(4000),
    reg_no character varying(15),
    reg_dt timestamp without time zone,
    area_cd character varying(8),
    city_cd character varying(8),
    district_cd character varying(8),
    state_cd character varying(8),
    country_cd character varying(8),
    loc_cd character varying(15),
    org_cd character varying(8) NOT NULL,
    grp_cd character varying(8) NOT NULL,
    record_status character(1),
    dw_last_updated_dt timestamp without time zone,
    dw_facility_cd character varying(16),
    dw_job_run_no numeric,
    dw_row_id character varying(128)
);

