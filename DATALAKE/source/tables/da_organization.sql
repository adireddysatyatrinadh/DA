
CREATE TABLE public.da_organization (
    org_cd character varying(8) NOT NULL,
    org_name character varying(64),
    org_desc character varying(64),
    grp_cd character varying(8) NOT NULL,
    record_status character(1),
    dw_last_updated_dt timestamp without time zone,
    dw_facility_cd character varying(16),
    dw_job_run_no numeric,
    dw_row_id character varying(128)
);

