CREATE TABLE public.da_doctor (
    doctor_cd character varying(8) NOT NULL,
    doctor_name character varying(60),
    record_status character varying(1),
    department character varying(8),
    loc_cd character varying,
    org_cd character varying(8) NOT NULL,
    grp_cd character varying(8) NOT NULL,
    dw_last_updated_dt timestamp without time zone,
    dw_facility_cd character varying(16),
    dw_job_run_no numeric,
    dw_row_id character varying(128)
);
