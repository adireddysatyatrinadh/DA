
CREATE TABLE public.da_gender (
    gender_cd character varying(32) NOT NULL,
    gender_name character varying(32),
    record_status character(1),
    loc_cd character varying,
    org_cd character varying(8) NOT NULL,
    grp_cd character varying(8) NOT NULL,
    dw_last_updated_dt timestamp without time zone,
    dw_facility_cd character varying(16),
    dw_job_run_no numeric,
    dw_row_id character varying(128)
);
