CREATE OR REPLACE FUNCTION public.fn_etl_dl_fact_consultation(ip_dw_facility_cd character varying, ip_dw_last_update_from_dt timestamp without t
ime zone, ip_dw_last_update_to_dt timestamp without time zone, INOUT op_status character varying DEFAULT NULL::character varying, INOUT op_error
_cd character varying DEFAULT NULL::character varying, INOUT op_error_msg character varying DEFAULT NULL::character varying)
 RETURNS record
 LANGUAGE plpgsql
AS $function$
/*
<DESCRIPTION>: dl_fact_consultation saving proc

<CHANGE_HISTORY>:
<DATE><AUTHOR><CHANGE_DESCRIPTION>
20-feb-2021   G.KeerthiCreated
20-feb-2021   murali    reviewed



select * from fn_etl_dl_fact_consultation('0004010003'::varchar,'2000-04-01 00:00:00'::timestamp,'2022-03-01 00:00:00'::timestamp);

select * from dl_fact_consultation where dw_facility_cd='0004010003'
except
select * from dl_fact_consultation2 where dw_facility_cd='0004010003';

select * from dl_fact_consultation2 where dw_facility_cd='0004010003'
except 
select * from  dl_fact_consultation where dw_facility_cd='0004010003';

select count(*) from dl_fact_consultation2 where dw_facility_cd='0004010003';

select count(*) from dl_fact_consultation where dw_facility_cd='0004010003';



select * from dl_fact_consultation2 where dw_row_id='0004010003-IP2021002370-BHC';

select * from dl_fact_admission where dw_row_id='0004010003-IP2021002370-BHC';


select * from dl_fact_admission2 where dw_row_id='0004010003-IP2021002370-BHC'
except
select * from dl_fact_admission where dw_row_id='0004010003-IP2021002370-BHC';


create unique index uk_dl_fact_consultation2_1 on dl_fact_consultation2(dw_row_id,dw_facility_cd)


*/
DECLARE
cur_cons cursor for
SELECT 
 dc.dw_row_id
,dc.umr_no
, pa.patient_name
, pa.gender_cd
, ge.gender_name
, pa.dob
, dc.age
,dc.area_cd
,ar.area_name
,dc.city_cd
, ct.city_name
, dc.district_cd
, ds.district_name
, dc.state_cd
, st.state_name
, dc.country_cd
, cn.country_name
, dc.bill_no
, dc.bill_dt
, dc.tran_no
, dc.tran_dt
, dc.bill_type_cd
,bt.bill_type_name
, dc.doctor_cd
, dd.doctor_name
, dc.speciality_cd
,dept.department_name as speciality_name
, dc.company_cd
, dco.company_name
, dc.company_type_cd
, dct.company_type_name
, dc.rec_type_cd
, drt.rec_type_name
, dc.consultation_type_cd
,cot.consultation_type_name
, dc.appointment_type_cd
, at.appointment_type_name
, dc.service_cd
, dc.service_cd as service_name
, dc.quantity
, dc.rate
, dc.amount
, dc.discount_amount
, dc.receipt_amount
,dal.loc_cd as loc_cd
,dal.loc_name as loc_name
,dal.loc_short_name as loc_short_name
,dal.org_cd as org_cd
,dal.org_name as org_name
,dal.org_short_name as org_short_name
,dal.grp_cd as grp_cd
,dal.grp_name as grp_name
,dal.grp_short_name as grp_short_name
,trim('0'||substring(dc.age,1,position('.' in dc.age)-1))::integer as age_in_years
,trim('0'||substring(dc.age,1,position('.' in dc.age)-1))::integer * 12+ trim('0'||substring(substring(dc.age,position('.' in dc.age)+1,100),1,p
osition('.' in substring(dc.age,position('.' in dc.age)+1,100))-1))::integer as age_in_months
,
(trim('0'||substring(dc.age,1,position('.' in dc.age)-1))::integer * 12+ trim('0'||substring(substring(dc.age,position('.' in dc.age)+1,100),1,p
osition('.' in substring(dc.age,position('.' in dc.age)+1,100))-1))::integer)*30
+
rtrim('0'||trim(substring(substring(dc.age,position('.' in dc.age)+1,100),position('.' in substring(dc.age,position('.' in dc.age)+1,100))+1,100
)),'.')::integer as age_in_days
, dc.dw_facility_cd
, dc.dw_job_run_no
, dc.dw_last_updated_dt
FROM public.da_consultation as dc
LEFT JOIN da_doctor dd ON(dc.doctor_cd=dd.doctor_cd and dc.dw_facility_cd=dd.dw_facility_cd)
LEFT JOIN da_company dco ON(dc.company_cd=dco.company_cd and dc.dw_facility_cd=dco.dw_facility_cd)
LEFT JOIN da_company_type dct ON(dc.company_type_cd=dct.company_type_cd and dc.dw_facility_cd=dct.dw_facility_cd)
LEFT JOIN da_rec_type drt ON(dc.rec_type_cd=drt.rec_type_cd and dc.dw_facility_cd=drt.dw_facility_cd)
LEFT JOIN v_dl_location_da_location_map AS dal ON(dc.dw_facility_cd=dal.dw_facility_cd)
LEFT JOIN da_patient as pa on (dc.umr_no = pa.umr_no and dc.dw_facility_cd = pa.dw_facility_cd)
LEFT JOIN da_gender as ge on (pa.gender_cd=ge.gender_cd and dc.dw_facility_cd = ge.dw_facility_cd)
LEFT JOIN da_country AS cn ON (dc.country_cd = cn.country_cd and dc.dw_facility_cd = cn.dw_facility_cd)
LEFT JOIN da_state AS st ON(dc.state_cd = st.state_cd and dc.dw_facility_cd = st.dw_facility_cd)
LEFT JOIN da_district AS ds ON(dc.district_cd = ds.district_cd and dc.dw_facility_cd = ds.dw_facility_cd)
LEFT JOIN da_city as ct  on(dc.city_cd = ct.city_cd and dc.dw_facility_cd = ct.dw_facility_cd)
LEFT JOIN da_area  as ar on (dc.area_cd = ar.area_cd and dc.dw_facility_cd = ar.dw_facility_cd)
LEFT JOIN da_bill_type  as bt on (bt.bill_type_cd = dc.bill_type_cd and dc.dw_facility_cd = bt.dw_facility_cd)
LEFT JOIN da_department as dept on(dept.department_cd=dc.speciality_cd and  dc.dw_facility_cd = dept.dw_facility_cd)
LEFT JOIN da_consultation_type as cot on (cot.consultation_type_cd=dc.consultation_type_cd and cot.dw_facility_cd=dc.dw_facility_cd)
LEFT JOIN da_appointment_type as at on (at.appointment_type_cd=dc.appointment_type_cd and at.dw_facility_cd=dc.dw_facility_cd)
where dc.dw_facility_cd = ip_dw_facility_cd 
and  dc.dw_last_updated_dt  between ip_dw_last_update_from_dt and ip_dw_last_update_to_dt;

lv_rec_cons RECORD;
v_state text;
v_msg text;
v_detail text;
v_hint text;
v_cont text;


BEGIN
OPEN cur_cons ;
LOOP 
FETCH NEXT FROM cur_cons INTO lv_rec_cons;
EXIT WHEN NOT FOUND;

lv_rec_cons.dw_last_updated_dt:= current_timestamp;

if (lower(trim(lv_rec_cons.company_cd))) = 'cash' then
 
if (lv_rec_cons.company_name is null or trim(lv_rec_cons.company_name)='') then
lv_rec_cons.company_name:='Cash';
end if;


if (lv_rec_cons.company_type_cd is null or trim(lv_rec_cons.company_type_cd)='') then
lv_rec_cons.company_type_cd:='Cash';
end if;


if (lv_rec_cons.company_type_name is null or trim(lv_rec_cons.company_type_name)='') then
lv_rec_cons.company_type_name:='Cash';
end if;
end if;

---UPDATE BLOCK
UPDATE dl_fact_consultation
SET dw_row_id= lv_rec_cons.dw_row_id,
umr_no= lv_rec_cons.umr_no,
patient_name= lv_rec_cons.patient_name,
gender_cd= lv_rec_cons.gender_cd,
gender_name= lv_rec_cons.gender_name,
dob= lv_rec_cons.dob,
age= lv_rec_cons.age,
area_cd= lv_rec_cons.area_cd,
area_name= lv_rec_cons.area_name,
city_cd= lv_rec_cons.city_cd,
city_name= lv_rec_cons.city_name,
district_cd= lv_rec_cons.district_cd,
district_name= lv_rec_cons.district_name,
state_cd= lv_rec_cons.state_cd,
state_name= lv_rec_cons.state_name,
country_cd= lv_rec_cons.country_cd,
country_name= lv_rec_cons.country_name,
bill_no= lv_rec_cons.bill_no,
bill_dt= lv_rec_cons.bill_dt,
tran_no= lv_rec_cons.tran_no,
tran_dt= lv_rec_cons.tran_dt,
bill_type_cd= lv_rec_cons.bill_type_cd,
bill_type_name= lv_rec_cons.bill_type_name,
doctor_cd= lv_rec_cons.doctor_cd,
doctor_name= lv_rec_cons.doctor_name,
speciality_cd= lv_rec_cons.speciality_cd,
speciality_name= lv_rec_cons.speciality_name,
company_cd= lv_rec_cons.company_cd,
company_name= lv_rec_cons.company_name,
company_type_cd= lv_rec_cons.company_type_cd,
company_type_name= lv_rec_cons.company_type_name,
rec_type_cd= lv_rec_cons.rec_type_cd,
rec_type_name= lv_rec_cons.rec_type_name,
consultation_type_cd= lv_rec_cons.consultation_type_cd,
consultation_type_name= lv_rec_cons.consultation_type_name,
appointment_type_cd= lv_rec_cons.appointment_type_cd,
appointment_type_name= lv_rec_cons.appointment_type_name,
service_cd= lv_rec_cons.service_cd,
service_name= lv_rec_cons.service_name,
quantity= lv_rec_cons.quantity,
rate= lv_rec_cons.rate,
amount= lv_rec_cons.amount,
discount_amount= lv_rec_cons.discount_amount,
receipt_amount= lv_rec_cons.receipt_amount,
loc_cd= lv_rec_cons.loc_cd,
loc_name= lv_rec_cons.loc_name,
loc_short_name= lv_rec_cons.loc_short_name,
org_cd= lv_rec_cons.org_cd,
org_name= lv_rec_cons.org_name,
org_short_name= lv_rec_cons.org_short_name,
grp_cd= lv_rec_cons.grp_cd,
grp_name= lv_rec_cons.grp_name,
grp_short_name= lv_rec_cons.grp_short_name,
age_in_years= lv_rec_cons.age_in_years,
age_in_months= lv_rec_cons.age_in_months,
age_in_days= lv_rec_cons.age_in_days,
dw_facility_cd= lv_rec_cons.dw_facility_cd,
dw_job_run_no= lv_rec_cons.dw_job_run_no,
dw_last_updated_dt= lv_rec_cons.dw_last_updated_dt
WHERE dw_row_id = lv_rec_cons.dw_row_id
AND dw_facility_cd = lv_rec_cons.dw_facility_cd;

--INSERT BLOCK
IF NOT FOUND  THEN
BEGIN

INSERT INTO dl_fact_consultation (
dw_row_id
,umr_no
,patient_name
,gender_cd
,gender_name
,dob
,age
,area_cd
,area_name
,city_cd
,city_name
,district_cd
,district_name
,state_cd
,state_name
,country_cd
,country_name
,bill_no
,bill_dt
,tran_no
,tran_dt
,bill_type_cd
,bill_type_name
,doctor_cd
,doctor_name
,speciality_cd
,speciality_name
,company_cd
,company_name
,company_type_cd
,company_type_name
,rec_type_cd
,rec_type_name
,consultation_type_cd
,consultation_type_name
,appointment_type_cd
,appointment_type_name
,service_cd
,service_name
,quantity
,rate
,amount
,discount_amount
,receipt_amount
,loc_cd 
,loc_name
,loc_short_name 
,org_cd
,org_name 
,org_short_name 
,grp_cd 
,grp_name 
,grp_short_name
,age_in_years
,age_in_months
,age_in_days
,dw_facility_cd
,dw_job_run_no
,dw_last_updated_dt )

VALUES(
lv_rec_cons.dw_row_id
,lv_rec_cons.umr_no
,lv_rec_cons.patient_name
,lv_rec_cons.gender_cd
,lv_rec_cons.gender_name
,lv_rec_cons.dob
,lv_rec_cons.age
,lv_rec_cons.area_cd
,lv_rec_cons.area_name
,lv_rec_cons.city_cd
,lv_rec_cons.city_name
,lv_rec_cons.district_cd
,lv_rec_cons.district_name
,lv_rec_cons.state_cd
,lv_rec_cons.state_name
,lv_rec_cons.country_cd
,lv_rec_cons.country_name
,lv_rec_cons.bill_no
,lv_rec_cons.bill_dt
,lv_rec_cons.tran_no
,lv_rec_cons.tran_dt
,lv_rec_cons.bill_type_cd
,lv_rec_cons.bill_type_name
,lv_rec_cons.doctor_cd
,lv_rec_cons.doctor_name
,lv_rec_cons.speciality_cd
,lv_rec_cons.speciality_name
,lv_rec_cons.company_cd
,lv_rec_cons.company_name
,lv_rec_cons.company_type_cd
,lv_rec_cons.company_type_name
,lv_rec_cons.rec_type_cd
,lv_rec_cons.rec_type_name
,lv_rec_cons.consultation_type_cd
,lv_rec_cons.consultation_type_name
,lv_rec_cons.appointment_type_cd
,lv_rec_cons.appointment_type_name
,lv_rec_cons.service_cd
,lv_rec_cons.service_name
,lv_rec_cons.quantity
,lv_rec_cons.rate
,lv_rec_cons.amount
,lv_rec_cons.discount_amount
,lv_rec_cons.receipt_amount
,lv_rec_cons.loc_cd 
,lv_rec_cons.loc_name
,lv_rec_cons.loc_short_name 
,lv_rec_cons.org_cd
,lv_rec_cons.org_name 
,lv_rec_cons.org_short_name 
,lv_rec_cons.grp_cd 
,lv_rec_cons.grp_name 
,lv_rec_cons.grp_short_name
,lv_rec_cons.age_in_years
,lv_rec_cons.age_in_months
,lv_rec_cons.age_in_days
,lv_rec_cons.dw_facility_cd
,lv_rec_cons.dw_job_run_no
,lv_rec_cons.dw_last_updated_dt );

END;
END IF;

END LOOP;
Close cur_cons;

---(Success Block...)
op_status:='0';
op_error_cd := Null;
op_error_msg:=Null;

--Exception Block....
Exception When Others Then get stacked diagnostics
v_state  = returned_sqlstate,
v_msg = message_text,
v_detail = pg_exception_detail,
v_hint = pg_exception_hint,
v_cont = pg_exception_context;

op_status:='1';
op_error_cd := v_state;
op_error_msg:=v_msg;
END;
