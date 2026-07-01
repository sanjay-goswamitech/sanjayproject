/*
database            : integrate_db
schema              : raw
storage integration : s3_integrations
file format         : format_csv
external stage      : s3_stage
bronze layer table  : csv_raw
stream              : csv_raw_stream
snowpipe            : csv_raw_pipe
silver layer schema : staging
table               : csv_raw_transaction
task                : task_load_csv_data
*/

-- database  
create database if not exists integrate_db;
use integrate_db;

-- schema 
create schema if not exists raw;
use schema raw;

-- storage integration 
create or replace storage integration s3_integrations
    type                      = external_stage
    storage_provider          = s3
    enabled                   = true
    storage_aws_role_arn      = 'arn:aws:iam::035625951902:role/snowflake_s3_role'
    storage_allowed_locations = ('s3://sanjay-project/');

-- take details 
desc integration s3_integrations;


-- file format 
create or replace file format integrate_db.raw.format_csv
    type        = csv
    skip_header = 1;


-- external stage
create or replace stage integrate_db.raw.s3_stage
    url                = 's3://sanjay-project/file_csv/'
    storage_integration = s3_integrations
    file_format        = (format_name = 'integrate_db.raw.format_csv');  

-- list of files in stage
list @s3_stage;






--     #############  bronze layer  #############  

 
-- create table : csv_raw
create or replace table integrate_db.raw.csv_raw (
    transaction_id varchar,  
    store_id varchar, 
    store_name varchar, 
    store_city varchar, 
    store_region varchar,
    cashier_id varchar, 
    customer_id varchar, 
    transaction_date date, 
    transaction_time time, 
    product_sku varchar,
    product_name varchar, 
    category varchar, 
    subcategory varchar, 
    quantity int, 
    unit_price float,
    discount_pct float,  
    total_amount  float, 
    payment_method varchar, 
    loyalty_points  int,
    load_ts  timestamp, 
    file_name  string 
);






--     #############  stream creation #############

-- stream 
create or replace stream integrate_db.raw.csv_raw_stream
    on table integrate_db.raw.csv_raw
    append_only = true;  

-- check stream details
desc stream integrate_db.raw.csv_raw_stream;
select * from integrate_db.raw.csv_raw_stream;




-- copy into 
copy into integrate_db.raw.csv_raw (
    transaction_id, store_id, store_name, store_city, store_region, cashier_id, customer_id, 
    transaction_date, transaction_time, product_sku, product_name, category, subcategory,
    quantity, unit_price, discount_pct, total_amount, payment_method, loyalty_points,
    load_ts, file_name)
from (
    select
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,  $11, $12, $13, $14, $15,
        $16, $17, $18, $19, current_timestamp(), metadata$filename
    from @integrate_db.raw.s3_stage )
file_format = (format_name = 'integrate_db.raw.format_csv')
on_error   = 'continue';

-- verify the data 
select * from integrate_db.raw.csv_raw;
select count(*) from integrate_db.raw.csv_raw;








--        #############  snowpipe creation #############  


-- snowpipe creation 
create or replace pipe integrate_db.raw.csv_raw_pipe
    auto_ingest = true
as
copy into integrate_db.raw.csv_raw (
    transaction_id, store_id, store_name, store_city, store_region,
    cashier_id, customer_id, transaction_date, transaction_time,
    product_sku, product_name, category, subcategory,
    quantity, unit_price, discount_pct, total_amount,
    payment_method, loyalty_points,
    load_ts,
    file_name
)
from (
    select
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
        $16, $17, $18, $19,  current_timestamp(), metadata$filename
    from @integrate_db.raw.s3_stage )
file_format = (format_name = 'integrate_db.raw.format_csv')
on_error   = 'continue';

-- check pipe details
desc pipe integrate_db.raw.csv_raw_pipe;

-- pipe status check
select system$pipe_status('integrate_db.raw.csv_raw_pipe');









--        #############  silver layer creation and load #############


-- schema : staging 
create schema if not exists integrate_db.staging;
use schema integrate_db.staging;

-- table: stg_csv_transaction
create or replace table integrate_db.staging.csv_raw_transaction (
    transaction_id  varchar, 
    store_id varchar,
    store_name varchar, 
    store_city varchar, 
    store_region varchar,
    cashier_id varchar,
    customer_id varchar,
    transaction_ts  timestamp, 
    product_sku varchar,
    product_name varchar, 
    category varchar, 
    subcategory varchar, 
    quantity int, 
    unit_price float,
    discount_pct float, 
    line_total float, 
    payment_method  varchar, 
    loyalty_points  int,
    source_file varchar, 
    processed_ts timestamp 
);


-- data transformation and load from stream to silver
insert into integrate_db.staging.csv_raw_transaction
select
    transaction_id,  
    store_id, 
    store_name, 
    store_city, 
    store_region, 
    cashier_id, 
    customer_id,
    -- combine date and time into timestamp
    to_timestamp(transaction_date || ' ' || transaction_time) as transaction_ts,
    product_sku, 
    product_name, 
    category, 
    subcategory,
    
    -- replace negative values with 0 for quantity, unit_price, discount_pct
    case when quantity > 0 then quantity else 0 end as quantity,
    case when unit_price > 0 then unit_price  else 0 end as unit_price,
    case when discount_pct > 0 then discount_pct else 0 end as discount_pct,

    -- line total = (qty * price) - discount amount
    (
        case when quantity > 0 then quantity else 0 end *
        case when unit_price > 0 then unit_price else 0 end
    )
    -
    (
        (
            case when quantity > 0 then quantity else 0 end *
            case when unit_price > 0 then unit_price else 0 end
        )
        * case when discount_pct > 0 then discount_pct else 0 end / 100
    ) as line_total,

    case
        when lower(payment_method) = 'credit card' then 'cc'
        when lower(payment_method) = 'debit card'  then 'dc'
        else payment_method
    end as payment_method,

    loyalty_points,
    file_name  as source_file,  
    current_timestamp() as processed_ts
from integrate_db.raw.csv_raw_stream;   

-- verify silver table
select * from integrate_db.staging.csv_raw_transaction;
select count(*) from integrate_db.staging.csv_raw_transaction;








--      #############  task  #############


-- task — automated stream to silver load
create or replace task integrate_db.staging.task_load_csv_data
    warehouse = compute_wh
    schedule  = '1 minute'
    when system$stream_has_data('integrate_db.raw.csv_raw_stream')
as
insert into integrate_db.staging.csv_raw_transaction
select
    transaction_id,  
    store_id, 
    store_name, 
    store_city, 
    store_region, 
    cashier_id, 
    customer_id,
    -- combine date and time into timestamp
    to_timestamp(transaction_date || ' ' || transaction_time) as transaction_ts,
    product_sku, 
    product_name, 
    category, 
    subcategory,
    
    -- replace negative values with 0 for quantity, unit_price, discount_pct
    case when quantity > 0 then quantity else 0 end as quantity,
    case when unit_price > 0 then unit_price  else 0 end as unit_price,
    case when discount_pct > 0 then discount_pct else 0 end as discount_pct,

    -- line total = (qty * price) - discount amount
    (
        case when quantity > 0 then quantity else 0 end *
        case when unit_price > 0 then unit_price else 0 end
    )
    -
    (
        (
            case when quantity > 0 then quantity else 0 end *
            case when unit_price > 0 then unit_price else 0 end
        )
        * case when discount_pct > 0 then discount_pct else 0 end / 100
    ) as line_total,

    case
        when lower(payment_method) = 'credit card' then 'cc'
        when lower(payment_method) = 'debit card'  then 'dc'
        else payment_method
    end as payment_method,

    loyalty_points,
    file_name  as source_file,  
    current_timestamp() as processed_ts
from integrate_db.raw.csv_raw_stream;

-- resume task to start automation  
alter task integrate_db.staging.task_load_csv_data resume;







-- final verification

-- 1. bronze count
select * from integrate_db.raw.csv_raw;
select count(*) from integrate_db.raw.csv_raw;

-- 2. silver count
select * from integrate_db.staging.csv_raw_transaction;
select count(*) from integrate_db.staging.csv_raw_transaction;

-- 3. stream empty or not
select system$stream_has_data('integrate_db.raw.csv_raw_stream');

-- 4. task succeeded?
select *
from table(integrate_db.information_schema.task_history(
    task_name => 'task_load_csv_data'
))
order by scheduled_time desc;


-- suspend task after verification
alter task integrate_db.staging.task_load_csv_data suspend;
