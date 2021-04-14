CREATE TABLE public.da_referal_type (
    referal_type_cd character varying(32) NOT NULL,
    referal_type_name character varying(32),
    referal_type_desc character varying(32),
    record_status character(1),
    loc_cd character varying,
    org_cd character varying(8) NOT NULL,
    grp_cd character varying(8) NOT NULL,
    dw_facility_cd character varying(16),
    dw_last_updated_dt timestamp without time zone,
    dw_job_run_no numeric,
    dw_row_id character varying(128)
);

