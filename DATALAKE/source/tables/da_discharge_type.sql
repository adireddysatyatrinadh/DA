CREATE TABLE public.da_discharge_type (
    discharge_type_cd character varying(32) NOT NULL,
    discharge_type_desc character varying(32),
    record_status character(1),
    grp_cd character varying(8) NOT NULL,
    dw_last_updated_dt timestamp without time zone,
    dw_facility_cd character varying(16),
    dw_job_run_no numeric,
    dw_row_id character varying(128)
);

