create table etl_job_schsch(
sch_cd  varchar(256)NOT NULL
,sch_name  varchar(256)
,sch_desc  varchar(512)
,sch_start_dt  timestamp 
,sch_end_dt  timestamp 
,sch_minute  varchar(256)
,sch_hour  varchar(256)
,sch_day_of_month  varchar(256)
,sch_month_of_year  varchar(256)
,sch_day_of_week  varchar(256)
,record_status  varchar(1)
,create_by  varchar(256)
,create_dt  timestamp 
,modify_by  varchar(256)
,modify_dt  timestamp 
,loc_cd  varchar(16)
,org_cd  varchar(16)
,grp_cd  varchar(16)not null
	
)