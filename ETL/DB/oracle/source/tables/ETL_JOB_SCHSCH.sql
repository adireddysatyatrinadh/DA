create table etl_job_schsch(
sch_cd  varchar2(64)  NOT NULL
,sch_name  varchar2(64) 
,sch_desc  varchar2(512)
,sch_start_dt  timestamp 
,sch_end_dt  timestamp 
,sch_minute  varchar2(200)
,sch_hour  varchar2(100)
,sch_day_of_month  varchar2(70)
,sch_month_of_year  varchar2(70)
,sch_day_of_week  varchar2(70)
,record_status  varchar2(1)
,create_by  varchar2(64)
,create_dt  timestamp 
,modify_by  varchar2(64)
,modify_dt  timestamp 
,loc_cd  varchar2(16)
,org_cd  varchar2(16)
,grp_cd  varchar2(16) not null
	
)