CREATE UNIQUE INDEX uk_da_company ON public.da_company USING btree (dw_row_id, dw_facility_cd);

CREATE UNIQUE INDEX uk_da_company_2 ON public.da_company USING btree (company_cd, dw_facility_cd);