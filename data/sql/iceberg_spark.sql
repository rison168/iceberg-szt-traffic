
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

-- 创建dwd表 主要是过滤重复数据，保证数据的正确性
create table IF NOT EXISTS szt_db.dwd_szt_data (
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

-- 查看deal_type 类型
-- spark-sql> SELECT collect_set(deal_type) FROM szt_db.dwd_szt_data;
-- 22/05/08 23:47:30 WARN HiveConf: HiveConf of name hive.mapred.supports.subdirectories does not exist
-- ["巴士","地铁出站","地铁入站"]
-- Time taken: 7.743 seconds, Fetched 1 row(s)

-- 创建dwd表，过滤巴士/不在运营数据的地铁出入站数据(只保留地铁刷卡数据)
create table IF NOT EXISTS szt_db.dwd_szt_subway_data (
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

INSERT OVERWRITE TABLE szt_db.dwd_szt_subway_data
SELECT
deal_date ,
close_date  ,
card_no ,
deal_value ,
deal_type ,
company_name ,
car_no ,
station ,
conn_mark ,
deal_money ,
equ_no
FROM
szt_db.dwd_szt_data
WHERE
deal_date != '巴士'
AND unix_timestamp(deal_date, 'yyyy-MM-dd HH:mm:ss') > unix_timestamp('2018-09-01 06:14:00', 'yyyy-MM-dd HH:mm:ss')
AND unix_timestamp(deal_date, 'yyyy-MM-dd HH:mm:ss') < unix_timestamp('2018-09-01 23:59:00', 'yyyy-MM-dd HH:mm:ss')
AND to_date(close_date) = '2018-09-01'
ORDER BY deal_date;

-- spark-sql> SELECT COUNT(*) FROM szt_db.dwd_szt_subway_data;
-- 22/05/09 01:03:01 WARN HiveConf: HiveConf of name hive.mapred.supports.subdirectories does not exist
-- 851867
-- Time taken: 2.787 seconds, Fetched 1 row(s)
