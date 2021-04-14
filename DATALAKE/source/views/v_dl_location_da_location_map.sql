
CREATE VIEW public.v_dl_location_da_location_map AS
 SELECT m.row_id,
    m.da_loc_cd AS loc_cd,
    dll.loc_name,
    dll.loc_short_name,
    m.da_org_cd AS org_cd,
    dlo.org_name,
    dlo.org_short_name,
    m.da_grp_cd AS grp_cd,
    dlg.grp_name,
    dlg.grp_short_name,
    m.source_grp_cd,
    m.source_org_cd,
    m.source_loc_cd,
    m.record_status,
    m.last_updated_dt,
    m.da_loc_cd AS dw_facility_cd
   FROM (((public.dl_location_da_location_map m
     JOIN public.dl_location dll ON ((((dll.loc_cd)::text = (m.da_loc_cd)::text) AND ((dll.record_status)::text = 'A'::text))))
     JOIN public.dl_organization dlo ON ((((dlo.org_cd)::text = (m.da_org_cd)::text) AND ((dlo.record_status)::text = 'A'::text))))
     JOIN public.dl_group dlg ON ((((dlg.grp_cd)::text = (m.da_grp_cd)::text) AND ((dlg.record_status)::text = 'A'::text))));

