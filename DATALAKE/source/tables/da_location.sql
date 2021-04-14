CREATE TABLE public.da_location (
    loc_cd character varying(8) NOT NULL,
    loc_name character varying(32),
    loc_desc character varying(32),
    org_cd character varying(8) NOT NULL,
    org_name character varying(64),
    grp_cd character varying(8) NOT NULL,
    grp_name character varying(64),
    record_status character(1),
    dw_last_updated_dt timestamp without time zone,
    dw_facility_cd character varying(16),
    dw_job_run_no numeric,
    dw_row_id character varying(128)
);

