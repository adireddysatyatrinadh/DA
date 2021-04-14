CREATE TABLE public.da_trn_source (
    trn_source_cd character varying(16),
    trn_source_name character varying(64),
    trn_source_desc character varying(128),
    record_status character(1),
    loc_cd character varying(16),
    org_cd character varying(8),
    grp_cd character varying(8),
    dw_facility_cd character varying(16),
    dw_job_run_no bigint,
    dw_last_updated_dt timestamp without time zone,
    dw_row_id character varying(128)
);

