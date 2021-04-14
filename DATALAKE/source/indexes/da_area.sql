CREATE INDEX idx_da_area ON public.da_area USING btree (dw_last_updated_dt, dw_facility_cd);

CREATE UNIQUE INDEX uk_da_area ON public.da_area USING btree (dw_row_id, dw_facility_cd);

CREATE UNIQUE INDEX uk_da_area_2 ON public.da_area USING btree (area_cd, dw_facility_cd);