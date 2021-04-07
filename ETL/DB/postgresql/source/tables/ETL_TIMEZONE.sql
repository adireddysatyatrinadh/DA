CREATE TABLE etl_timezone
(
    timezone_cd varchar(8) NOT NULL,
    timezone_name varchar(128) NOT NULL,
    utc_offset varchar(8),
    utc_minutes integer,
    record_status varchar(1),
    create_by varchar(32),
    create_dt timestamp,
    modify_by varchar(32),
    modify_dt timestamp,
    loc_cd varchar(16),
    org_cd varchar(16),
    grp_cd varchar(16) NOT NULL
  
)



