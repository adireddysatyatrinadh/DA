
CREATE TABLE public.da_consultation (
    umr_no character varying(15),
    bill_no character varying(15),
    bill_dt timestamp without time zone,
    tran_no character varying(15),
    tran_dt timestamp without time zone,
    bill_type_cd character varying(33),
    doctor_cd character varying(15),
    speciality_cd character varying(8),
    company_cd character varying(8),
    company_type_cd character varying(15),
    rec_type_cd character varying(1),
    consultation_type_cd character varying(9),
    appointment_type_cd character varying,
    service_cd character varying,
    quantity numeric,
    rate numeric,
    amount numeric,
    discount_amount numeric,
    receipt_amount numeric,
    loc_cd character varying(15),
    org_cd character varying(8),
    grp_cd character varying(8),
    dw_last_updated_dt timestamp without time zone,
    dw_facility_cd character varying(16),
    dw_job_run_no numeric,
    dw_row_id character varying(128),
    age character varying(16),
    area_cd character varying(8),
    city_cd character varying(8),
    district_cd character varying(8),
    state_cd character varying(8),
    country_cd character varying(8)
);

