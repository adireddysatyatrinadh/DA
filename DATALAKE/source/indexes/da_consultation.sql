CREATE UNIQUE INDEX idx_da_consultation ON public.da_consultation USING btree (dw_row_id, dw_facility_cd);

CREATE INDEX idx_da_consultation_1 ON public.da_consultation USING btree (dw_last_updated_dt, dw_facility_cd);

CREATE UNIQUE INDEX uk_da_consultation ON public.da_consultation USING btree (dw_row_id, dw_facility_cd);

CREATE UNIQUE INDEX uk_da_consultation_2 ON public.da_consultation USING btree (bill_no, dw_facility_cd);