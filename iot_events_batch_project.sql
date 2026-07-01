/*
database            : integrate_db
schema              : raw
file format         : format_json
external stage      : s3_json_stage
bronze layer table  : json_raw
stream              : json_raw_stream
snowpipe            : json_raw_pipe
silver layer schema : staging
table               : stg_json_sensor
task                : task_load_sensor_data
*/


-- database  
use integrate_db;

-- schema 
use schema raw;

-- file format
create or replace file format integrate_db.raw.format_json
    type              = json
    strip_outer_array = true;


-- external stage
create or replace stage integrate_db.raw.s3_json_stage
    url                 = 's3://sanjay-project/file_json/'
    storage_integration = s3_integrations
    file_format         = (format_name = 'integrate_db.raw.format_json');

-- check files
list @integrate_db.raw.s3_json_stage;






--        #############  bronze layer  #############


-- create table : json_raw
create or replace table integrate_db.raw.json_raw (
    event_id varchar, 
    event_type varchar, 
    store_id varchar, 
    store_name varchar,
    event_ts timestamp,
    device_id varchar, 
    raw_payload  variant, 
    load_ts timestamp, 
    source_file varchar    
);






--        #############  stream  #############

-- stream: json_raw_stream
create or replace stream integrate_db.raw.json_raw_stream
    on table integrate_db.raw.json_raw
    append_only = true;

-- check stream details
desc stream integrate_db.raw.json_raw_stream;
select system$stream_has_data('integrate_db.raw.json_raw_stream');




-- copy into bronze table from stage
copy into integrate_db.raw.json_raw (
    event_id, event_type, store_id, store_name,
    event_ts, device_id, raw_payload, load_ts, source_file)
from ( 
select
        $1:event_id::varchar, $1:event_type::varchar, $1:store_id::varchar,
        $1:store_name::varchar, $1:timestamp::timestamp, $1:device_id::varchar,
        $1, current_timestamp(), metadata$filename        
from @integrate_db.raw.s3_json_stage)
file_format = (format_name = integrate_db.raw.format_json)
on_error    = 'continue';

-- verify bronze load
select * from integrate_db.raw.json_raw;
select count(*) from integrate_db.raw.json_raw;







--         #############  snowpipe  #############


-- snowpipe: json_raw_pipe
create or replace pipe integrate_db.raw.json_raw_pipe
    auto_ingest = true
as
copy into integrate_db.raw.json_raw (
    event_id, event_type, store_id, store_name,
    event_ts, device_id, raw_payload, load_ts, source_file)
from ( 
select
        $1:event_id::varchar, $1:event_type::varchar, $1:store_id::varchar,
        $1:store_name::varchar, $1:timestamp::timestamp, $1:device_id::varchar,
        $1, current_timestamp(), metadata$filename        
from @integrate_db.raw.s3_json_stage)
file_format = (format_name = integrate_db.raw.format_json)
on_error    = 'continue';

-- pipe details check karo
desc pipe integrate_db.raw.json_raw_pipe;

-- pipe status
select system$pipe_status('integrate_db.raw.json_raw_pipe');









--         #############  silver layer  #############


-- schema : staging
create schema if not exists integrate_db.staging;
use schema integrate_db.staging;

-- table: stg_json_sensor
create or replace table integrate_db.staging.stg_json_sensor (
    event_id varchar,
    event_type varchar, 
    store_id varchar,
    store_name varchar,
    event_ts timestamp, 
    device_id varchar, 
    firmware varchar,
    battery_pct int, 
    signal_rssi int,
    store_floor int, 
    sensor_name varchar, 
    sensor_value  float,
    sensor_unit varchar,  
    source_file varchar, 
    processed_ts  timestamp
);


-- data transformation and load from stream to silver
insert into integrate_db.staging.stg_json_sensor
select
    s.raw_payload:event_id::varchar  as event_id,
    s.raw_payload:event_type::varchar as event_type,
    s.raw_payload:store_id::varchar as store_id,
    s.raw_payload:store_name::varchar as store_name,
    s.raw_payload:timestamp::timestamp as event_ts,
    s.raw_payload:device_id::varchar as device_id,
    s.raw_payload:metadata:firmware::varchar as firmware, 
    s.raw_payload:metadata:battery_pct::int as battery_pct,
    s.raw_payload:metadata:signal_rssi::int as signal_rssi,
    s.raw_payload:metadata:store_floor::int  as store_floor,
    f.value:sensor::varchar as sensor_name,
    f.value:value::float as sensor_value,
    f.value:unit::varchar as sensor_unit,
    s.source_file  as source_file,
    current_timestamp()  as processed_ts
from integrate_db.raw.json_raw_stream as s,
lateral flatten(input => s.raw_payload:readings) as f;

-- verify silver
select * from integrate_db.staging.stg_json_sensor;
select count(*) from integrate_db.staging.stg_json_sensor;

-- stream empty or not
select system$stream_has_data('integrate_db.raw.json_raw_stream');







--    ###########  task #############


-- task — automatic stream to silver
create or replace task integrate_db.staging.task_load_sensor_data
    warehouse = compute_wh
    schedule  = '1 minute'
    when system$stream_has_data('integrate_db.raw.json_raw_stream')
as
insert into integrate_db.staging.stg_json_sensor
select
    s.raw_payload:event_id::varchar,
    s.raw_payload:event_type::varchar,
    s.raw_payload:store_id::varchar,
    s.raw_payload:store_name::varchar,
    s.raw_payload:timestamp::timestamp,
    s.raw_payload:device_id::varchar,
    s.raw_payload:metadata:firmware::varchar,
    s.raw_payload:metadata:battery_pct::int,
    s.raw_payload:metadata:signal_rssi::int,
    s.raw_payload:metadata:store_floor::int,
    f.value:sensor::varchar,
    f.value:value::float,
    f.value:unit::varchar,
    s.source_file,
    current_timestamp()
from integrate_db.raw.json_raw_stream  as s,
lateral flatten(input => s.raw_payload:readings) as f;

-- task resume
alter task integrate_db.staging.task_load_sensor_data resume;









--  ########## final verification  ############

-- 1. bronze count
select * from integrate_db.raw.json_raw;
select count(*)  from integrate_db.raw.json_raw;

-- 2. silver count
select * from integrate_db.staging.stg_json_sensor;
select count(*)  from integrate_db.staging.stg_json_sensor;

-- 3. stream empty or not
select system$stream_has_data('integrate_db.raw.json_raw_stream');

-- 4. task succeeded?
select *
from table(integrate_db.information_schema.task_history(
    task_name => 'task_load_sensor_data'
))
order by scheduled_time desc
limit 3;


-- suspend task after verification
alter task integrate_db.staging.task_load_sensor_data suspend;



-- silver file-wise count
select 
    source_file,
    count(*) as row_count,
    min(processed_ts) as processed_at
from integrate_db.staging.stg_json_sensor
group by source_file
order by processed_at;




select 'bronze csv' as layer, count(*) as `rows` from integrate_db.raw.csv_raw
union all select 'bronze parquet ', count(*) from integrate_db.raw.parquet_raw
union all select 'bronze json', count(*) from integrate_db.raw.json_raw
union all select 'silver csv', count(*) from integrate_db.staging.csv_raw_transaction
union all select 'silver parquet', count(*) from integrate_db.staging.stg_parquet_order
union all select 'silver json', count(*) from integrate_db.staging.stg_json_sensor;
