CREATE TABLE public.dl_location (
    row_id character varying(128),
    loc_cd character varying(32),
    loc_name character varying(128),
    loc_short_name character varying(16),
    org_cd character varying(16),
    grp_cd character varying(8),
    record_status character varying(1),
    last_updated_dt timestamp without time zone
);

