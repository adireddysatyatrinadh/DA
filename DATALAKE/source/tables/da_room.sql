CREATE TABLE public.da_room (
    room_cd character varying(15),
    room_name character varying(64),
    nursestation_cd character varying(15),
    ward_cd character varying(8) NOT NULL,
    block_cd character varying(2),
    floor_cd character varying(15),
    record_status character(1),
    loc_cd character varying(15),
    org_cd character varying(8) NOT NULL,
    grp_cd character varying(8) NOT NULL,
    dw_facility_cd character varying(16),
    dw_last_updated_dt timestamp without time zone,
    dw_job_run_no numeric,
    dw_row_id character varying(128)
);

