
create database IF NOT EXISTS szt_db;

-- 创建ods表，不做改动直接加载
create table IF NOT EXISTS szt_db.ods_szt_data (
deal_date String,
close_date timestamp ,
card_no String,
deal_value String,
deal_type String,
company_name String,
car_no String,
station String,
conn_mark String,
deal_money String,
equ_no String
) using iceberg
partitioned by (days(close_date))
;

