CREATE TABLE public.da_department (
    department_cd character varying(8) NOT NULL,
    department_desc character varying(64),
    department_name character varying(64),
    depttype character(1),
    record_status character(1),
    loc_cd character varying(15),
    org_cd character varying(8) NOT NULL,
    grp_cd character varying(8) NOT NULL,
    dw_last_updated_dt timestamp without time zone,
    dw_facility_cd character varying(16),
    dw_job_run_no numeric,
    dw_row_id character varying(128)
);

