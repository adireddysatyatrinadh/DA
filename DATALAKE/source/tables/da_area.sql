CREATE TABLE public.da_area (
    area_cd character varying(8) NOT NULL,
    area_name character varying(64),
    area_desc character varying(64),
    city_cd character varying(8),
    district_cd character varying(8),
    state_cd character varying(8),
    country_cd character varying(8),
    grp_cd character varying(15) NOT NULL,
    record_status character(1),
    dw_last_updated_dt timestamp without time zone,
    dw_facility_cd character varying(16),
    dw_row_id character varying(128),
    dw_job_run_no numeric
);

