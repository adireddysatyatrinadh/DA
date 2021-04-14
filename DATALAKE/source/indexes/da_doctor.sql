CREATE INDEX idx_da_doctor ON public.da_doctor USING btree (dw_last_updated_dt, dw_facility_cd);

CREATE UNIQUE INDEX uk_da_doctor ON public.da_doctor USING btree (dw_row_id, dw_facility_cd);

CREATE UNIQUE INDEX uk_da_doctor_2 ON public.da_doctor USING btree (doctor_cd, dw_facility_cd);