CREATE OR REPLACE FUNCTION public.fn_etl_dl_fact_admission(ip_dw_facility_cd character varying, ip_dw_last_update_from_dt timestamp without time
 zone, ip_dw_last_update_to_dt timestamp without time zone, INOUT op_status character varying DEFAULT NULL::character varying, INOUT op_error_cd
 character varying DEFAULT NULL::character varying, INOUT op_error_msg character varying DEFAULT NULL::character varying)
 RETURNS record
 LANGUAGE plpgsql
AS $function$
/*
--DROP FUNCTION fn_etl_dl_fact_admission
<DESCRIPTION>: pr_dl_fact_admission saving proc

<CHANGE_HISTORY>:
<DATE><AUTHOR><CHANGE_DESCRIPTION>
20-feb-2021   G.KeerthiCreated
20-feb-2021   murali    reviewed
select * from fn_etl_dl_fact_admission('0004010003'::varchar,'2000-04-01 00:00:00'::timestamp,'2021-03-15 00:00:00'::timestamp);

select * from dl_fact_admission where dw_facility_cd='0004010003'
except
select * from dl_fact_admission2 where dw_facility_cd='0004010003';

select * from dl_fact_admission2 where dw_facility_cd='0004010003'
except 
select * from  dl_fact_admission where dw_facility_cd='0004010003';

select count(*) from dl_fact_admission2 where dw_facility_cd='0004010003';

select count(*) from dl_fact_admission where dw_facility_cd='0004010003';



select * from dl_fact_admission2 where dw_row_id='0004010003-IP2021002370-BHC';

select * from dl_fact_admission where dw_row_id='0004010003-IP2021002370-BHC';


select * from dl_fact_admission2 where dw_row_id='0004010003-IP2021002370-BHC'
except
select * from dl_fact_admission where dw_row_id='0004010003-IP2021002370-BHC';


create unique index uk_dl_fact_admission2_1 on dl_fact_admission2(dw_row_id,dw_facility_cd)
*/
DECLARE
cur_admn cursor FOR 
SELECT
 ad.dw_row_id
 , ad.admn_no
, ad.admn_dt
, ad.umr_no
, ad.er_no
, ad.er_dt
, ad.trn_source_cd
, ts.trn_source_name
, ad.er_case_type_cd
, ect.er_case_type_name
,pa.patient_name
,pa.gender_cd
,ge.gender_name
,pa.dob
,ad.age
,pa.reg_no
,pa.reg_dt
, ad.area_cd
, ar.area_name
, ad.city_cd
, ct.city_name
, ad.district_cd
, ds.district_name
, ad.state_cd
, st.state_name
, ad.country_cd
, cn.country_name
, ad.company_cd
, co.company_name
, ad.company_type_cd
, cot.company_type_name
, ad.admission_type_cd
, at.admission_type_name
, ad.department_cd
, dt.department_name
, ad.doctor_cd
, doc.doctor_name
, ad.ward_cd
, w.ward_name
, ad.room_cd
, rom.room_name
, ad.rec_type_cd
, rec.rec_type_name
,dal.loc_cd as loc_cd
,dal.loc_name as loc_name
,dal.loc_short_name as loc_short_name
,dal.org_cd as org_cd
,dal.org_name as org_name
,dal.org_short_name as org_short_name
,dal.grp_cd as grp_cd
,dal.grp_name as grp_name
,dal.grp_short_name as grp_short_name
,trim('0'||substring(ad.age,1,position('.' in ad.age)-1))::integer as age_in_years
,trim('0'||substring(ad.age,1,position('.' in ad.age)-1))::integer * 12+ trim('0'||substring(substring(ad.age,position('.' in ad.age)+1,100),1,p
osition('.' in substring(ad.age,position('.' in ad.age)+1,100))-1))::integer as age_in_months
,
(trim('0'||substring(ad.age,1,position('.' in ad.age)-1))::integer * 12+ trim('0'||substring(substring(ad.age,position('.' in ad.age)+1,100),1,p
osition('.' in substring(ad.age,position('.' in ad.age)+1,100))-1))::integer)*30
+
rtrim('0'||trim(substring(substring(ad.age,position('.' in ad.age)+1,100),position('.' in substring(ad.age,position('.' in ad.age)+1,100))+1,100
)),'.')::integer as age_in_days
, ad.record_status
, ad.dw_facility_cd
, ad.dw_job_run_no
, ad.dw_last_updated_dt
FROM public.da_admn AS ad
LEFT JOIN da_patient as pa ON(ad.umr_no = pa.umr_no and ad.dw_facility_cd = pa.dw_facility_cd)
LEFT JOIN da_country AS cn ON (ad.country_cd = cn.country_cd and ad.dw_facility_cd = cn.dw_facility_cd)
LEFT JOIN da_state AS st ON(ad.state_cd = st.state_cd and ad.dw_facility_cd = st.dw_facility_cd)
LEFT JOIN da_district AS ds ON(ad.district_cd = ds.district_cd and ad.dw_facility_cd = ds.dw_facility_cd)
LEFT JOIN da_city as ct  on(ad.city_cd = ct.city_cd and ad.dw_facility_cd = ct.dw_facility_cd)
LEFT JOIN da_area  as ar on (ad.area_cd = ar.area_cd and ad.dw_facility_cd = ar.dw_facility_cd)
LEFT JOIN da_gender as ge on (pa.gender_cd = ge.gender_cd and ge.dw_facility_cd = pa.dw_facility_cd)
LEFT JOIN da_company as co on (ad.company_cd = co.company_cd and ad.dw_facility_cd = co.dw_facility_cd)
LEFT JOIN da_department as dt on(ad.department_cd= dt.department_cd and ad.dw_facility_cd = dt.dw_facility_cd)
LEFT JOIN da_doctor as doc on (ad.doctor_cd = doc.doctor_cd and ad.dw_facility_cd = doc.dw_facility_cd)
LEFT JOIN da_room as rom on(ad.room_cd = rom.room_cd and rom.ward_cd = ad.ward_cd and ad.dw_facility_cd = rom.dw_facility_cd) 
LEFT JOIN da_rec_type as rec on(ad.rec_type_cd = rec.rec_type_cd and ad.dw_facility_cd = rec.dw_facility_cd)
LEFT JOIN v_dl_location_da_location_map AS dal ON(ad.dw_facility_cd=dal.dw_facility_cd)
LEFT JOIN da_trn_source as ts ON (ad.dw_facility_cd=ts.dw_facility_cd and ad.trn_source_cd=ts.trn_source_cd)
LEFT JOIN da_er_case_type ect on (ect.dw_facility_cd=ad.dw_facility_cd and ect.er_case_type_cd=ad.er_case_type_cd)
LEFT JOIN da_company_type as cot on (cot.dw_facility_cd=ad.dw_facility_cd and cot.company_type_cd=ad.company_type_cd)
LEFT JOIN da_admission_type as at on (at.dw_facility_cd=ad.dw_facility_cd and at.admission_type_cd=ad.admission_type_cd)
LEFT JOIN da_ward w on (w.ward_cd=ad.ward_cd and ad.dw_facility_cd=w.dw_facility_cd)
where ad.dw_facility_cd = ip_dw_facility_cd 
  and ad.dw_last_updated_dt  between ip_dw_last_update_from_dt and ip_dw_last_update_to_dt;


lv_rec_admn RECORD;


        v_state text;
v_msg text;
v_detail text;
v_hint text;
v_cont text;


BEGIN
OPEN cur_admn ;
LOOP 
FETCH NEXT FROM cur_admn INTO lv_rec_admn;
EXIT WHEN NOT FOUND;
lv_rec_admn.dw_last_updated_dt:= current_timestamp;

if (lower(trim(lv_rec_admn.company_cd))) = 'cash' then
 
if (lv_rec_admn.company_name is null or trim(lv_rec_admn.company_name)='') then
lv_rec_admn.company_name:='Cash';
end if;


if (lv_rec_admn.company_type_cd is null or trim(lv_rec_admn.company_type_cd)='') then
lv_rec_admn.company_type_cd:='Cash';
end if;


if (lv_rec_admn.company_type_name is null or trim(lv_rec_admn.company_type_name)='') then
lv_rec_admn.company_type_name:='Cash';
end if;
end if;

---UPDATE BLOCK
UPDATE dl_fact_admission
SET dw_row_id = lv_rec_admn.dw_row_id,
admn_no = lv_rec_admn.admn_no,
admn_dt = lv_rec_admn.admn_dt,
umr_no = lv_rec_admn.umr_no,
er_no = lv_rec_admn.er_no,
er_dt = lv_rec_admn.er_dt,
trn_source_cd = lv_rec_admn.trn_source_cd,
trn_source_name = lv_rec_admn.trn_source_name,
er_case_type_cd = lv_rec_admn.er_case_type_cd,
er_case_type_name = lv_rec_admn.er_case_type_name,
patient_name = lv_rec_admn.patient_name,
gender_cd = lv_rec_admn.gender_cd,
gender_name = lv_rec_admn.gender_name, 
dob = lv_rec_admn.dob,
age = lv_rec_admn.age,
reg_no = lv_rec_admn.reg_no,
reg_dt = lv_rec_admn.reg_dt,
area_cd = lv_rec_admn.area_cd,
area_name = lv_rec_admn.area_name,
city_cd = lv_rec_admn.city_cd,
city_name = lv_rec_admn.city_name ,
district_cd = lv_rec_admn.district_cd,
district_name = lv_rec_admn.district_name,
state_cd = lv_rec_admn.state_cd,
state_name = lv_rec_admn.state_name,
country_cd =  lv_rec_admn.country_cd,
country_name = lv_rec_admn.country_name,
company_cd = lv_rec_admn.company_cd,
company_name = lv_rec_admn.company_name,
company_type_cd = lv_rec_admn.company_type_cd,
company_type_name = lv_rec_admn.company_type_name,
admission_type_cd = lv_rec_admn.admission_type_cd,
admission_type_name = lv_rec_admn.admission_type_name,
department_cd = lv_rec_admn.department_cd,
department_name = lv_rec_admn.department_name,
doctor_cd = lv_rec_admn.doctor_cd,
doctor_name = lv_rec_admn.doctor_name,
ward_cd = lv_rec_admn.ward_cd,
ward_name = lv_rec_admn.ward_name,
room_cd = lv_rec_admn.room_cd,
room_name = lv_rec_admn.room_name,
rec_type_cd = lv_rec_admn.rec_type_cd, 
rec_type_name = lv_rec_admn.rec_type_name,
loc_cd = lv_rec_admn.loc_cd,
loc_name = lv_rec_admn.loc_name, 
loc_short_name = lv_rec_admn.loc_short_name, 
org_cd = lv_rec_admn.org_cd, 
org_name = lv_rec_admn.org_name,
org_short_name = lv_rec_admn.org_short_name, 
grp_cd = lv_rec_admn.grp_cd, 
grp_name = lv_rec_admn.grp_name,
grp_short_name = lv_rec_admn.grp_short_name, 
dw_last_updated_dt = lv_rec_admn.dw_last_updated_dt,
dw_facility_cd = lv_rec_admn.dw_facility_cd,
dw_job_run_no = lv_rec_admn.dw_job_run_no,
age_in_days = lv_rec_admn.age_in_days, 
age_in_months = lv_rec_admn.age_in_months,
age_in_years = lv_rec_admn.age_in_years
WHERE dw_row_id = lv_rec_admn.dw_row_id
AND dw_facility_cd = lv_rec_admn.dw_facility_cd;

--INSERT BLOCK
IF NOT FOUND  THEN
BEGIN

INSERT INTO dl_fact_admission (
 dw_row_id
,admn_no
,admn_dt
,umr_no
,er_no
,er_dt
,trn_source_cd
,trn_source_name
,er_case_type_cd
,er_case_type_name
,patient_name
,gender_cd
,gender_name
,dob
,age
,reg_no
,reg_dt
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
,company_cd
,company_name
,company_type_cd
,company_type_name
,admission_type_cd
,admission_type_name
,department_cd
,department_name
,doctor_cd
,doctor_name
,ward_cd
,ward_name
,room_cd
,room_name
,rec_type_cd
,rec_type_name
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
,record_status
,dw_facility_cd
,dw_job_run_no
,dw_last_updated_dt)
VALUES(
lv_rec_admn.dw_row_id
,lv_rec_admn.admn_no
,lv_rec_admn.admn_dt
,lv_rec_admn.umr_no
,lv_rec_admn.er_no
,lv_rec_admn.er_dt
,lv_rec_admn.trn_source_cd
,lv_rec_admn.trn_source_name
,lv_rec_admn.er_case_type_cd
,lv_rec_admn.er_case_type_name
,lv_rec_admn.patient_name
,lv_rec_admn.gender_cd
,lv_rec_admn.gender_name
,lv_rec_admn.dob
,lv_rec_admn.age
,lv_rec_admn.reg_no
,lv_rec_admn.reg_dt
,lv_rec_admn.area_cd
,lv_rec_admn.area_name
,lv_rec_admn.city_cd
,lv_rec_admn.city_name
,lv_rec_admn.district_cd
,lv_rec_admn.district_name
,lv_rec_admn.state_cd
,lv_rec_admn.state_name
,lv_rec_admn.country_cd
,lv_rec_admn.country_name
,lv_rec_admn.company_cd
,lv_rec_admn.company_name
,lv_rec_admn.company_type_cd
,lv_rec_admn.company_type_name
,lv_rec_admn.admission_type_cd
,lv_rec_admn.admission_type_name
,lv_rec_admn.department_cd
,lv_rec_admn.department_name
,lv_rec_admn.doctor_cd
,lv_rec_admn.doctor_name
,lv_rec_admn.ward_cd
,lv_rec_admn.ward_name
,lv_rec_admn.room_cd
,lv_rec_admn.room_name
,lv_rec_admn.rec_type_cd
,lv_rec_admn.rec_type_name
,lv_rec_admn.loc_cd 
,lv_rec_admn.loc_name 
,lv_rec_admn.loc_short_name 
,lv_rec_admn.org_cd 
,lv_rec_admn.org_name 
,lv_rec_admn.org_short_name
,lv_rec_admn.grp_cd 
,lv_rec_admn.grp_name 
,lv_rec_admn.grp_short_name 
,lv_rec_admn.age_in_years
,lv_rec_admn.age_in_months
,lv_rec_admn.age_in_days
,lv_rec_admn.record_status
,lv_rec_admn.dw_facility_cd
,lv_rec_admn.dw_job_run_no
,lv_rec_admn.dw_last_updated_dt
);

END;
END IF;

END LOOP;
Close cur_admn;

---(Success Block...)
op_status:='0';
op_error_cd := Null;
op_error_msg:=Null;

--Exception Block....
Exception When Others Then 
get stacked diagnostics
v_state  = returned_sqlstate,
v_msg = message_text,
v_detail = pg_exception_detail,
v_hint = pg_exception_hint,
v_cont = pg_exception_context;

op_status:='1';
op_error_cd := v_state;
op_error_msg:=v_msg;
END;
