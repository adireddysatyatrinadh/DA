CREATE TABLE public.dl_organization (
    row_id character varying(128),
    org_cd character varying(16),
    org_name character varying(128),
    org_short_name character varying(16),
    grp_cd character varying(8),
    record_status character varying(1),
    last_updated_dt timestamp without time zone
);
