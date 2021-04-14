CREATE OR REPLACE FUNCTION public.fn_etl_dl_fact_registration(ip_dw_facility_cd character varying, ip_dw_last_update_from_dt timestamp without t
ime zone, ip_dw_last_update_to_dt timestamp without time zone, INOUT op_status character varying DEFAULT NULL::character varying, INOUT op_error
_cd character varying DEFAULT NULL::character varying, INOUT op_error_msg character varying DEFAULT NULL::character varying)
 RETURNS record
 LANGUAGE plpgsql
AS $function$
/*
<DESCRIPTION>: pr_dl_fact_registration saving proc

<CHANGE_HISTORY>:
<DATE><AUTHOR><CHANGE_DESCRIPTION>
20-feb-2021   G.KeerthiCreated
20-feb-2021   murali    reviewed


select * from fn_etl_dl_fact_registration('0004010003'::varchar,'2000-04-01 00:00:00'::timestamp,'2022-03-01 00:00:00'::timestamp);

select * from dl_fact_registration where dw_facility_cd='0004010003'
except
select * from dl_fact_registration2 where dw_facility_cd='0004010003';

select * from dl_fact_registration2 where dw_facility_cd='0004010003'
except 
select * from  dl_fact_registration where dw_facility_cd='0004010003';

select count(*) from dl_fact_registration2 where dw_facility_cd='0004010003';

select count(*) from dl_fact_registration where dw_facility_cd='0004010003';



select * from dl_fact_registration2 where dw_row_id='0004010003-IP2021002370-BHC';

select * from dl_fact_registration where dw_row_id='0004010003-IP2021002370-BHC';


select * from dl_fact_registration2 where dw_row_id='0004010003-IP2021002370-BHC'
except
select * from dl_fact_registration where dw_row_id='0004010003-IP2021002370-BHC';


create unique index uk_dl_fact_registration2_1 on dl_fact_registration2(dw_row_id,dw_facility_cd)

*/
DECLARE
cur_reg cursor FOR
SELECT 
 re.dw_row_id
,re.umr_no
,pa.patient_name
,pa.gender_cd
,ge.gender_name
,pa.dob
,re.age
,re.reg_no
,re.reg_dt
,re.referal_source_cd
,drs.referal_source_name
,re.area_cd
,ar.area_name
,re.city_cd
,ct.city_name
,re.district_cd
,ds.district_name
,re.state_cd
,st.state_name
,re.country_cd
,cn.country_name
,re.nationality_cd
,dn.nationality_name
,re.company_cd
,co.company_name
,re.company_type_cd
,dct.company_type_name
,re.expiry_dt
,re.is_expired
,re.trn_source_cd
,ts.trn_source_name
,re.rec_type_cd
,drt.rec_type_name
,re.record_status
,dal.loc_cd as loc_cd
,dal.loc_name as loc_name
,dal.loc_short_name as loc_short_name
,dal.org_cd as org_cd
,dal.org_name as org_name
,dal.org_short_name as org_short_name
,dal.grp_cd as grp_cd
,dal.grp_name as grp_name
,dal.grp_short_name as grp_short_name
,trim('0'||substring(re.age,1,position('.' in re.age)-1))::integer as age_in_years
,trim('0'||substring(re.age,1,position('.' in re.age)-1))::integer * 12+ trim('0'||substring(substring(re.age,position('.' in re.age)+1,100),1,p
osition('.' in substring(re.age,position('.' in re.age)+1,100))-1))::integer as age_in_months
,
(trim('0'||substring(re.age,1,position('.' in re.age)-1))::integer * 12+ trim('0'||substring(substring(re.age,position('.' in re.age)+1,100),1,p
osition('.' in substring(re.age,position('.' in re.age)+1,100))-1))::integer)*30
+
rtrim('0'||trim(substring(substring(re.age,position('.' in re.age)+1,100),position('.' in substring(re.age,position('.' in re.age)+1,100))+1,100
)),'.')::integer as age_in_days
,re.dw_facility_cd
,re.dw_job_run_no
,re.dw_last_updated_dt 
FROM public.da_reg AS re
LEFT JOIN da_country as cn ON(re.country_cd=cn.country_cd and re.dw_facility_cd=cn.dw_facility_cd)
LEFT JOIN da_state AS st ON (re.state_cd = st.state_cd and re.dw_facility_cd=st.dw_facility_cd)
LEFT JOIN da_district AS ds ON(re.district_cd=ds.district_cd and re.dw_facility_cd=ds.dw_facility_cd)
LEFT JOIN da_city AS ct ON(re.dw_facility_cd=ct.dw_facility_cd and re.city_cd=ct.city_cd) 
LEFT JOIN da_area AS ar ON(re.dw_facility_cd=ar.dw_facility_cd and re.area_cd=ar.area_cd)
LEFT JOIN da_company AS co ON(re.company_cd=co.company_cd and re.dw_facility_cd=co.dw_facility_cd)
LEFT JOIN da_referal_source AS drs ON(re.referal_source_cd=drs.referal_source_cd and re.dw_facility_cd=drs.dw_facility_cd)
LEFT JOIN da_nationality AS dn ON(re.nationality_cd=dn.nationality_cd and re.dw_facility_cd=dn.dw_facility_cd)
LEFT JOIN da_company_type AS dct ON (re.company_type_cd=dct.company_type_cd and re.dw_facility_cd=dct.dw_facility_cd)
LEFT JOIN da_rec_type AS drt ON(re.rec_type_cd=drt.rec_type_cd and re.dw_facility_cd=drt.dw_facility_cd)
LEFT JOIN v_dl_location_da_location_map AS dal ON(re.dw_facility_cd=dal.dw_facility_cd)
LEFT JOIN da_patient as pa ON(re.umr_no = pa.umr_no and re.dw_facility_cd = pa.dw_facility_cd)
LEFT JOIN da_gender as ge ON(re.dw_facility_cd = ge.dw_facility_cd and ge.gender_cd=pa.gender_cd)
LEFT JOIN da_trn_source as ts ON (re.dw_facility_cd=ts.dw_facility_cd and re.trn_source_cd=ts.trn_source_cd)
where re.dw_facility_cd = ip_dw_facility_cd 
and  re.dw_last_updated_dt  between ip_dw_last_update_from_dt and ip_dw_last_update_to_dt;

lv_rec_reg RECORD;
v_state text;
v_msg text;
v_detail text;
v_hint text;
v_cont text;


BEGIN

--RAISE NOTICE '1';
OPEN cur_reg ;
LOOP 
FETCH NEXT FROM cur_reg INTO lv_rec_reg;

--RAISE NOTICE '11';

EXIT WHEN NOT FOUND;
lv_rec_reg.dw_last_updated_dt:= current_timestamp;

if (lower(trim(lv_rec_reg.company_cd))) = 'cash' then
 
if (lv_rec_reg.company_name is null or trim(lv_rec_reg.company_name)='') then
lv_rec_reg.company_name:='Cash';
end if;


if (lv_rec_reg.company_type_cd is null or trim(lv_rec_reg.company_type_cd)='') then
lv_rec_reg.company_type_cd:='Cash';
end if;


if (lv_rec_reg.company_type_name is null or trim(lv_rec_reg.company_type_name)='') then
lv_rec_reg.company_type_name:='Cash';
end if;
end if;


---UPDATE BLOCK
UPDATE dl_fact_registration
SET  dw_row_id = lv_rec_reg.dw_row_id
,umr_no =lv_rec_reg.umr_no 
,patient_name = lv_rec_reg.patient_name 
,gender_cd = lv_rec_reg.gender_cd
,gender_name = lv_rec_reg.gender_name
,dob = lv_rec_reg.dob
,age =lv_rec_reg.age
,reg_no = lv_rec_reg.reg_no 
,reg_dt = lv_rec_reg.reg_dt
,referal_source_cd = lv_rec_reg.referal_source_cd
,referal_source_name = lv_rec_reg.referal_source_name 
,area_cd = lv_rec_reg.area_cd 
,area_name = lv_rec_reg.area_name 
,city_cd =  lv_rec_reg.city_cd 
,city_name = lv_rec_reg.city_name
,district_cd = lv_rec_reg.district_cd
,district_name = lv_rec_reg.district_name
,state_cd = lv_rec_reg.state_cd 
,state_name = lv_rec_reg.state_name
,country_cd = lv_rec_reg.country_cd 
,country_name = lv_rec_reg.country_name
,nationality_cd = lv_rec_reg.nationality_cd
,nationality_name = lv_rec_reg.nationality_name
,company_cd = lv_rec_reg.company_cd 
,company_name = lv_rec_reg.company_name
,company_type_cd = lv_rec_reg.company_type_cd
,company_type_name = lv_rec_reg.company_type_name
,expiry_dt = lv_rec_reg.expiry_dt 
,is_expired = lv_rec_reg.is_expired
,trn_source_cd = lv_rec_reg.trn_source_cd
,trn_source_name = lv_rec_reg.trn_source_name
,rec_type_cd = lv_rec_reg.rec_type_cd
,rec_type_name = lv_rec_reg.rec_type_name
,record_status = lv_rec_reg.record_status
,loc_cd = lv_rec_reg.loc_cd
,loc_name = lv_rec_reg.loc_name
,loc_short_name = lv_rec_reg.loc_short_name
,org_cd = lv_rec_reg.org_cd
,org_name = lv_rec_reg.org_name 
,org_short_name = lv_rec_reg.org_short_name
,grp_cd = lv_rec_reg.grp_cd 
,grp_name = lv_rec_reg.grp_name 
,grp_short_name = lv_rec_reg.grp_short_name
,age_in_years = lv_rec_reg.age_in_years
,age_in_months = lv_rec_reg.age_in_months
,age_in_days = lv_rec_reg.age_in_days
,dw_facility_cd = lv_rec_reg.dw_facility_cd
,dw_job_run_no = lv_rec_reg.dw_job_run_no 
,dw_last_updated_dt = lv_rec_reg.dw_last_updated_dt
WHERE dw_row_id = lv_rec_reg.dw_row_id
AND dw_facility_cd =lv_rec_reg.dw_facility_cd;

--INSERT BLOCK
IF NOT FOUND  THEN
BEGIN

INSERT INTO dl_fact_registration (
dw_row_id
,umr_no
,patient_name
,gender_cd
,gender_name
,dob
,age
,reg_no
,reg_dt
,referal_source_cd
,referal_source_name
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
,nationality_cd
,nationality_name
,company_cd
,company_name
,company_type_cd
,company_type_name
,expiry_dt
,is_expired
,trn_source_cd
,trn_source_name
,rec_type_cd
,rec_type_name
,record_status
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
 lv_rec_reg.dw_row_id
,lv_rec_reg.umr_no
,lv_rec_reg.patient_name
,lv_rec_reg.gender_cd
,lv_rec_reg.gender_name
,lv_rec_reg.dob
,lv_rec_reg.age
,lv_rec_reg.reg_no
,lv_rec_reg.reg_dt
,lv_rec_reg.referal_source_cd
,lv_rec_reg.referal_source_name
,lv_rec_reg.area_cd
,lv_rec_reg.area_name
,lv_rec_reg.city_cd
,lv_rec_reg.city_name
,lv_rec_reg.district_cd
,lv_rec_reg.district_name
,lv_rec_reg.state_cd
,lv_rec_reg.state_name
,lv_rec_reg.country_cd
,lv_rec_reg.country_name
,lv_rec_reg.nationality_cd
,lv_rec_reg.nationality_name
,lv_rec_reg.company_cd
,lv_rec_reg.company_name
,lv_rec_reg.company_type_cd
,lv_rec_reg.company_type_name
,lv_rec_reg.expiry_dt
,lv_rec_reg.is_expired
,lv_rec_reg.trn_source_cd
,lv_rec_reg.trn_source_name
,lv_rec_reg.rec_type_cd
,lv_rec_reg.rec_type_name
,lv_rec_reg.record_status
,lv_rec_reg.loc_cd 
,lv_rec_reg.loc_name
,lv_rec_reg.loc_short_name 
,lv_rec_reg.org_cd 
,lv_rec_reg.org_name 
,lv_rec_reg.org_short_name 
,lv_rec_reg.grp_cd
,lv_rec_reg.grp_name 
,lv_rec_reg.grp_short_name 
,lv_rec_reg.age_in_years
,lv_rec_reg.age_in_months
,lv_rec_reg.age_in_days
,lv_rec_reg.dw_facility_cd
,lv_rec_reg.dw_job_run_no
,lv_rec_reg.dw_last_updated_dt );

END;
END IF;

--RAISE NOTICE '111';
END LOOP;
Close cur_reg;
--RAISE NOTICE '1111';

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
