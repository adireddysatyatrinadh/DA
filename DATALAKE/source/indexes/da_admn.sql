
CREATE UNIQUE INDEX idx_da_admn ON public.da_admn USING btree (dw_row_id, dw_facility_cd);

CREATE INDEX idx_da_admn_1 ON public.da_admn USING btree (dw_last_updated_dt, dw_facility_cd);

CREATE UNIQUE INDEX idx_da_admn_2 ON public.da_admn USING btree (admn_no, dw_facility_cd);

CREATE UNIQUE INDEX uk_da_admn ON public.da_admn USING btree (dw_row_id, dw_facility_cd);
