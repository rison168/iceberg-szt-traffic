
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

--创建dwd表，地铁入站刷卡数据
create table IF NOT EXISTS szt_db.dwd_szt_subway_in_data (
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

--创建dwd表，地铁出站刷卡数据
create table IF NOT EXISTS szt_db.dwd_szt_subway_out_data (
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

INSERT OVERWRITE TABLE szt_db.dwd_szt_subway_in_data
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
FROM szt_db.dwd_szt_subway_data
WHERE to_date(close_date) = '2018-09-01' AND deal_type = '地铁入站'
ORDER BY deal_date;


INSERT OVERWRITE TABLE szt_db.dwd_szt_subway_out_data
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
FROM szt_db.dwd_szt_subway_data
WHERE to_date(close_date) = '2018-09-01' AND deal_type = '地铁出站'
ORDER BY deal_date;


-- dws 建宽表每天刷卡记录宽表
CREATE TABLE IF NOT EXISTS szt_db.dws_szt_card_record_wide(
card_no STRING,
deal_date_arr ARRAY < STRING > ,
deal_value_arr ARRAY < STRING > ,
deal_type_arr ARRAY < STRING > ,
company_name_arr ARRAY < STRING > ,
station_arr ARRAY < STRING > ,
conn_mark_arr ARRAY < STRING > ,
deal_money_arr ARRAY < STRING > ,
equ_no_arr ARRAY < STRING > ,
count BIGINT,
day TIMESTAMP
) USING ICEBERG
PARTITIONED BY(days(day))

INSERT OVERWRITE TABLE szt_db.dws_szt_card_record_wide
SELECT card_no,
       collect_list(deal_date),
       collect_list(deal_value),
       collect_list(deal_type),
       collect_list(company_name),
       collect_list(station),
       collect_list(conn_mark),
       collect_list(deal_money),
       collect_list(equ_no),
       count(*) AS card_no_count,
       CAST ('2018-09-01' AS  TIMESTAMP)
FROM szt_db.dwd_szt_subway_data
WHERE to_date(close_date) = '2018-09-01'
GROUP BY card_no
ORDER BY card_no_count DESC;

--ADS 业务表 当天出、入地铁排名明细表
CREATE TABLE IF NOT EXISTS szt_db.ads_szt_in_station_top(
station STRING,
deal_date_arr ARRAY < STRING > ,
card_no_arr ARRAY < STRING > ,
company_name_arr ARRAY < STRING > ,
equ_no_arr ARRAY < STRING > ,
count BIGINT,
day TIMESTAMP
) USING ICEBERG
PARTITIONED BY (days(day));

CREATE TABLE IF NOT EXISTS szt_db.ads_szt_out_station_top(
station STRING,
deal_date_arr ARRAY < STRING > ,
card_no_arr ARRAY < STRING > ,
company_name_arr ARRAY < STRING > ,
equ_no_arr ARRAY < STRING > ,
count BIGINT,
day TIMESTAMP
) USING ICEBERG
PARTITIONED BY (days(day));


INSERT OVERWRITE TABLE szt_db.ads_szt_in_station_top
SELECT
station,
collect_list(deal_date),
collect_list(card_no),
collect_list(company_name),
collect_list(equ_no),
count(*) AS count,
CAST ('2018-09-01' AS TIMESTAMP)
FROM szt_db.dwd_szt_subway_in_data
WHERE to_date(close_date) = '2018-09-01'
GROUP By station
ORDER BY count DESC;

INSERT OVERWRITE TABLE szt_db.ads_szt_out_station_top
SELECT
station,
collect_list(deal_date),
collect_list(card_no),
collect_list(company_name),
collect_list(equ_no),
count(*) AS count,
CAST ('2018-09-01' AS TIMESTAMP)
FROM szt_db.dwd_szt_subway_out_data
WHERE to_date(close_date) = '2018-09-01'
GROUP BY station
ORDER BY count DESC;

-- 当日消费排行

CREATE TABLE IF NOT EXISTS szt_db.ads_szt_card_deal_top(
card_no STRING,
deal_date_arr ARRAY<STRING>,
deal_value_sum DOUBLE,
company_name_arr ARRAY<STRING>,
station_arr ARRAY<STRING>,
conn_mark_arr ARRAY<STRING>,
deal_money_sum DOUBLE,
equ_no_arr ARRAY<STRING>,
count bigint,
day TIMESTAMP
) USING ICEBERG
PARTITIONED BY (days(day))
;

INSERT OVERWRITE TABLE szt_db.ads_szt_card_deal_top
SELECT
a.card_no,
a.deal_date_arr,
b.deal_value_sum,
a.company_name_arr,
a.station_arr,
a.conn_mark_arr,
c.deal_money_sum,
a.equ_no_arr,
a.count,
CAST ('2018-09-01' AS  TIMESTAMP)
FROM szt_db.dws_szt_card_record_wide AS a
LEFT JOIN (SELECT
         card_no,
         SUM(deal_v) AS deal_value_sum
     FROM szt_db.dws_szt_card_record_wide
     LATERAL VIEW explode(deal_value_arr) as deal_v GROUP BY card_no) AS b  ON a.card_no = b.card_no
LEFT JOIN (SELECT
          card_no,
          SUM(deal_m) AS deal_money_sum
      FROM szt_db.dws_szt_card_record_wide
      LATERAL VIEW explode(deal_money_arr) as deal_m GROUP BY card_no) AS c ON a.card_no = c.card_no
WHERE to_date(a.day) = '2018-09-01'
ORDER BY b.deal_value_sum DESC
;



