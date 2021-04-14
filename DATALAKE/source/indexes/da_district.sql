CREATE INDEX idx_da_district_1 ON public.da_district USING btree (dw_last_updated_dt, dw_facility_cd);

CREATE UNIQUE INDEX uk_da_district ON public.da_district USING btree (dw_row_id, dw_facility_cd);

CREATE UNIQUE INDEX uk_da_district_2 ON public.da_district USING btree (district_cd, dw_facility_cd);