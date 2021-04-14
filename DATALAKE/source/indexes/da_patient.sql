CREATE INDEX idx_da_patient_1 ON public.da_patient USING btree (dw_last_updated_dt, dw_facility_cd);

CREATE UNIQUE INDEX uk_da_patient ON public.da_patient USING btree (dw_row_id, dw_facility_cd);

CREATE UNIQUE INDEX uk_da_patient_2 ON public.da_patient USING btree (umr_no, dw_facility_cd);