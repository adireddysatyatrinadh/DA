CREATE INDEX uk_dl_fact_admission ON public.dl_fact_admission USING btree (loc_cd, dw_last_updated_dt);

CREATE UNIQUE INDEX uk_dl_fact_admission2_1 ON public.dl_fact_admission USING btree (dw_row_id, dw_facility_cd);


