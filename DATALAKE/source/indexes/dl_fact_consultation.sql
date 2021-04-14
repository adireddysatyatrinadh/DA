CREATE INDEX uk_dl_fact_consultation ON public.dl_fact_consultation USING btree (loc_cd, dw_last_updated_dt);

CREATE UNIQUE INDEX uk_dl_fact_consultation2_1 ON public.dl_fact_consultation USING btree (dw_row_id, dw_facility_cd);