
CREATE INDEX uk_dl_fact_registration ON public.dl_fact_registration USING btree (loc_cd, dw_last_updated_dt);

CREATE UNIQUE INDEX uk_dl_fact_registration2_1 ON public.dl_fact_registration USING btree (dw_row_id, dw_facility_cd);

