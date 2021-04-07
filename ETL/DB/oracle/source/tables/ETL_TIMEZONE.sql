CREATE TABLE etl_timezone
(
    timezone_cd varchar2(8) NOT NULL,
    timezone_name varchar2(128) NOT NULL,
    utc_offset varchar(8),
    utc_minutes integer,
    record_status varchar2(1),
    create_by varchar2(32),
    create_dt timestamp,
    modify_by varchar2(32),
    modify_dt timestamp,
    loc_cd varchar2(16),
    org_cd varchar2(16),
    grp_cd varchar2(16) NOT NULL
  
)


"IST", "Asia/Kolkata"		"05:30:00"	330   01-JAN-2021,'etl',null,null,0,0,0