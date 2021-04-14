CREATE INDEX idx_da_city ON public.da_city USING btree (dw_last_updated_dt, dw_facility_cd);

CREATE UNIQUE INDEX uk_da_city ON public.da_city USING btree (dw_row_id, dw_facility_cd);

CREATE UNIQUE INDEX uk_da_city_2 ON public.da_city USING btree (city_cd, dw_facility_cd);