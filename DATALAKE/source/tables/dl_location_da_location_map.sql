CREATE TABLE public.dl_location_da_location_map (
    row_id character varying(128) NOT NULL,
    da_loc_cd character varying(64),
    da_org_cd character varying(8),
    da_grp_cd character varying(8),
    source_grp_cd character varying(10),
    source_org_cd character varying(10),
    source_loc_cd character varying(10),
    record_status character varying(1),
    last_updated_dt timestamp without time zone,
    dw_facility_cd character varying(16)
);
