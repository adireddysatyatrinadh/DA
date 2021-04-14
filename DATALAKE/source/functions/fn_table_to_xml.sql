CREATE OR REPLACE FUNCTION public.fn_table_to_xml(ip_table_name character varying, ip_destination_folder_name character varying DEFAULT '/data':
:character varying, INOUT op_status character varying DEFAULT NULL::character varying, INOUT op_error_cd character varying DEFAULT NULL::charact
er varying, INOUT op_error_msg character varying DEFAULT NULL::character varying)
 RETURNS record
 LANGUAGE plpgsql
AS $function$
/*

select * from fn_table_to_xml('dl_fact_registration','/data');
select * from fn_table_to_xml('dl_fact_consultation','/data');
select * from fn_table_to_xml('dl_fact_admission','/data');
*/
DECLARE
lv_sql VARCHAR(4000);
lv_count INTEGER;


v_state text;
v_msg text;
v_detail text;
v_hint text;
v_cont text;



lv_file_id integer:=0;
lv_cur_count integer:=0;
lv_file_count integer :=20000;
lv_tot_count integer;
lv_offset integer;
BEGIN


lv_sql :='select count(*) from '||ip_table_name;

EXECUTE lv_sql INTO lv_tot_count; 

RAISE NOTICE '%',lv_tot_count;

while (lv_cur_count < lv_tot_count) 
loop
  lv_file_id :=lv_file_id +1;
  lv_cur_count =lv_cur_count+lv_file_count;
  lv_offset = (lv_file_id-1)*lv_file_count;
  if (lv_offset+lv_file_count > lv_tot_count) then
      lv_file_count=lv_tot_count-lv_offset;
  end if;

  lv_sql:='copy (select query_to_xml(''select * from '||ip_table_name||' order by ctid offset '||to_char(lv_offset,'9999999')||' rows fetch next
 '||to_char(lv_file_count,'9999999')||'  row only'',true,false,'''')) to '''||ip_destination_folder_name||'/'||ip_table_name||'_'||to_char(lv_fi
le_id,'FM00')||'.xml'' csv';
  RAISE NOTICE '%',lv_sql;
  EXECUTE lv_sql; 
end loop;
 
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
