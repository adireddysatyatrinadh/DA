CREATE TABLE da_room (
    room_cd varchar2(15),
    room_name varchar2(64),
    nursestation_cd varchar2(15),
    ward_cd varchar2(8) NOT NULL,
    block_cd varchar2(2),
    floor_cd varchar2(15),
    record_status char(1),
    loc_cd varchar2(15),
    org_cd varchar2(8) NOT NULL,
    grp_cd varchar2(8) NOT NULL,
    dw_facility_cd varchar2(16),
    dw_last_updated_dt date,
    dw_job_run_no integer,
    dw_row_id varchar2(128)
);

