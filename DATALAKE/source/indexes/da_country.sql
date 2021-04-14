CREATE INDEX idx_da_country_1 ON public.da_country USING btree (dw_last_updated_dt, dw_facility_cd);

CREATE UNIQUE INDEX uk_da_country ON public.da_country USING btree (dw_row_id, dw_facility_cd);

CREATE UNIQUE INDEX uk_da_country_2 ON public.da_country USING btree (country_cd, dw_facility_cd);
