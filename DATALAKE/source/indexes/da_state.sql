CREATE INDEX idx_da_state_1 ON public.da_state USING btree (dw_last_updated_dt, dw_facility_cd);


CREATE UNIQUE INDEX uk_da_state ON public.da_state USING btree (dw_row_id, dw_facility_cd);

CREATE UNIQUE INDEX uk_da_state_2 ON public.da_state USING btree (state_cd, dw_facility_cd);


