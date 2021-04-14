CREATE UNIQUE INDEX uk_da_company_type ON public.da_company_type USING btree (dw_row_id, dw_facility_cd);

CREATE UNIQUE INDEX uk_da_company_type_2 ON public.da_company_type USING btree (company_type_cd, dw_facility_cd);