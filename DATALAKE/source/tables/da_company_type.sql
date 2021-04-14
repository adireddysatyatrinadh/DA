CREATE TABLE public.da_company_type (
    company_type_cd character varying(15) NOT NULL,
    company_type_name character varying(64),
    record_status character(1),
    loc_cd character varying,
    org_cd character varying(15) NOT NULL,
    grp_cd character varying(15) NOT NULL,
    dw_last_updated_dt timestamp without time zone,
    dw_facility_cd character varying(16),
    dw_job_run_no numeric,
    dw_row_id character varying(128)
);

