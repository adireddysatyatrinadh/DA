CREATE TABLE public.da_rec_type (
    rec_type_cd character(1),
    rec_type_name character(5),
    rec_type_desc character(5),
    record_status character(1),
    loc_cd character varying,
    org_cd character varying(8) NOT NULL,
    grp_cd character varying(8) NOT NULL,
    dw_last_updated_dt timestamp without time zone,
    dw_facility_cd character varying(16),
    dw_job_run_no numeric,
    dw_row_id character varying(128)
);