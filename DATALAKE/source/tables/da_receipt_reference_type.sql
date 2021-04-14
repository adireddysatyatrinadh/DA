
CREATE TABLE public.da_receipt_reference_type (
    receipt_reference_type_cd character varying(32),
    receipt_reference_type_name character varying(64),
    receipt_reference_type_desc character varying(128),
    record_status character(1),
    loc_cd character varying(16),
    org_cd character varying(8),
    grp_cd character varying(8),
    dw_last_updated_dt timestamp without time zone,
    dw_facility_cd character varying(16),
    dw_job_run_no bigint,
    dw_row_id character varying(128)
);