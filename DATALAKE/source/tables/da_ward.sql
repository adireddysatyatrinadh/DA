CREATE TABLE public.da_ward (
    ward_cd character varying(8),
    ward_name character varying(32),
    ward_group_cd character varying(8),
    record_status character(1),
    loc_cd character varying(16),
    org_cd character varying(8),
    grp_cd character varying(8),
    dw_facility_cd character varying(16),
    dw_job_run_no bigint,
    dw_last_updated_dt timestamp without time zone,
    dw_row_id character varying(128)
);

