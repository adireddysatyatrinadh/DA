CREATE UNIQUE INDEX idx_da_reg ON public.da_reg USING btree (dw_row_id, dw_facility_cd);

CREATE INDEX idx_da_reg_1 ON public.da_reg USING btree (dw_last_updated_dt, dw_facility_cd);

CREATE UNIQUE INDEX uk_da_reg ON public.da_reg USING btree (dw_row_id, dw_facility_cd);

CREATE UNIQUE INDEX uk_da_reg_2 ON public.da_reg USING btree (reg_no, dw_facility_cd);