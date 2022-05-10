package com.rison.szt.spark

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @PACKAGE_NAME: com.rison.szt.spark
 * @NAME: SubwayCardRecordWide
 * @USER: Rison
 * @DATE: 2022/5/10 11:20
 * @PROJECT_NAME: iceberg-szt-traffic
 * 深圳通刷卡记录宽表
 * */

object SubwayCardRecordWideApplication extends Logging{
  def main(args: Array[String]): Unit = {
    val date = "2018-09-01"
    val sparkConf = new SparkConf()
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
      .set("spark.sql.catalog.local.type", "hadoop")
      .set("spark.sql.catalog.local.warehouse", "hdfs:///apps/hive/warehouse")
      .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .set("spark.sql.catalog.spark_catalog.type", "hive")
      .set("spark.sql.catalog.spark_catalog.uri", "thrift://tbds-10-1-0-84:9083")
      .set("spark.sql.parquet.binaryAsString", "true")
      .set("spark.sql.session.timeZone", "CST")
      .set("spark.sql.warehouse.dir", "/apps/hive/warehouse")
      .set("spark.sql.catalog.catalog-name.default-namespace", "default")
      .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
      //      .setMaster("local[*]")
      .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    logInfo("======SubwayCardRecordWideApplication start ...  ========")
    val sql =
      s"""
        |INSERT OVERWRITE TABLE spark_catalog.szt_db.dws_szt_card_record_wide
        |SELECT card_no,
        |       collect_list(deal_date),
        |       collect_list(deal_value),
        |       collect_list(deal_type),
        |       collect_list(company_name),
        |       collect_list(station),
        |       collect_list(conn_mark),
        |       collect_list(deal_money),
        |       collect_list(equ_no),
        |       count(*) AS card_no_count,
        |       CAST ('$date' AS  TIMESTAMP)
        |FROM spark_catalog.szt_db.dwd_szt_subway_data
        |WHERE to_date(close_date) = '$date'
        |GROUP BY card_no
        |ORDER BY card_no_count DESC;
        |""".stripMargin
    sparkSession.sql(sql)
    val frame: DataFrame = sparkSession.sql("SELECT * FROM spark_catalog.szt_db.dws_szt_card_record_wide").cache()
    frame.show(100)
    val cardCount: Long = frame.count()
    logInfo(s"=============SubwayCardRecordWideApplication finish ! cardCount = $cardCount ===================")
    sparkSession.close()
  }

}
/**
/usr/hdp/2.2.0.0-2041/spark/bin/spark-submit  --class com.rison.szt.spark.SubwayCardRecordWideApplication \
    --master yarn \
    --deploy-mode client \
    --driver-memory 500m \
    --executor-memory 500m \
    --executor-cores 1 \
    --queue default \
    /root/spark-dir/szt-spark.jar
 **/

/**
+---------+--------------------+--------------------+--------------------------------+--------------------------------+--------------------------------+--------------------+--------------------+--------------------+-----+-------------------+
|  card_no|       deal_date_arr|      deal_value_arr|                   deal_type_arr|                company_name_arr|                     station_arr|       conn_mark_arr|      deal_money_arr|          equ_no_arr|count|                day|
+---------+--------------------+--------------------+--------------------------------+--------------------------------+--------------------------------+--------------------+--------------------+--------------------+-----+-------------------+
|HHJJJAEIH|[2018-09-01 08:54...|[300, 200, 0, 0, ...|[地铁出站, 地铁出站, 地铁入站...|[地铁五号线, 地铁七号线, 地铁...|  [太安, 华强南, 太安, 华强南...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[263035104, 26502...|   21|2018-09-01 00:00:00|
|HHAAJCJCD|[2018-09-01 08:22...|[200, 0, 0, 200, ...|[地铁出站, 地铁入站, 地铁入站...|[地铁七号线, 地铁七号线, 地铁...|  [桃源村, 桃源村, 桃源村, 深...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[265016103, 26501...|   18|2018-09-01 00:00:00|
|FIJGADBIC|[2018-09-01 08:41...|[0, 0, 300, 300, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁一号线, 地铁一号线, 地铁...|  [科学馆站, 罗湖站, 罗湖站, ...|[0, 0, 0, 0, 0, 0...|[0, 0, 285, 285, ...|[268005124, 26800...|   16|2018-09-01 00:00:00|
|HHAAACIFE|[2018-09-01 10:25...|[0, 0, 200, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁二号线, 地铁七号线, 地铁...|  [安托山, 桃源村, 农林, 深云...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[260025110, 26501...|   16|2018-09-01 00:00:00|
|HHAAAJJCH|[2018-09-01 10:25...|[0, 0, 200, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁二号线, 地铁七号线, 地铁...|  [安托山, 深云, 农林, 桃源村...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[260025111, 26501...|   16|2018-09-01 00:00:00|
|HHJJJFBEI|[2018-09-01 10:25...|[0, 0, 200, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁二号线, 地铁七号线, 地铁...|  [安托山, 桃源村, 农林, 深云...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[260025112, 26501...|   16|2018-09-01 00:00:00|
|HHAAAACHC|[2018-09-01 10:23...|[0, 800, 200, 0, ...|[地铁入站, 地铁出站, 地铁出站...|[地铁二号线, 地铁二号线, 地铁...|   [东角头, 湾厦, 登良, 湾厦,...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[260015115, 26001...|   14|2018-09-01 00:00:00|
|HHAAACICH|[2018-09-01 09:05...|[0, 0, 200, 600, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁九号线, 地铁四号线, 地铁...|  [鹿丹村, 红岭北, 向西村, 红...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[267029102, 26201...|   14|2018-09-01 00:00:00|
|HHAAAJJFJ|[2018-09-01 10:00...|[0, 0, 200, 400, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁三号线, 地铁三号线, 地铁...|  [丹竹头, 大运, 六约, 丹竹头...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[261021118, 26102...|   14|2018-09-01 00:00:00|
|HHABAEFJG|[2018-09-01 09:12...|[400, 0, 0, 300, ...|[地铁出站, 地铁入站, 地铁入站...|[地铁三号线, 地铁一号线, 地铁...|  [老街, 会展中心站, 老街, 竹...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[261012153, 26800...|   14|2018-09-01 00:00:00|
|HHABJFIFD|[2018-09-01 10:33...|[0, 0, 200, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁一号线, 地铁一号线, 地铁...|[华强路站, 科学馆站, 大剧院站...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[268006112, 26801...|   14|2018-09-01 00:00:00|
|HHAAABHAF|[2018-09-01 08:10...|[0, 500, 200, 0, ...|[地铁入站, 地铁出站, 地铁出站...|[地铁一号线, 地铁二号线, 地铁...|  [鲤鱼门, 侨香, 安托山, 侨香...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[268027109, 26002...|   13|2018-09-01 00:00:00|
|HHAAJJBBJ|[2018-09-01 10:38...|[0, 0, 200, 300, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁九号线, 地铁九号线, 地铁...|   [梅景, 人民南, 景田, 银湖,...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[267018123, 26703...|   13|2018-09-01 00:00:00|
|HHAAJJDHJ|[2018-09-01 07:27...|[0, 0, 500, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁一号线, 地铁一号线, 地铁...| [机场东, 宝安中心, 宝安中心,...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[268036110, 26803...|   13|2018-09-01 00:00:00|
|HHJJAGDJH|[2018-09-01 09:59...|[0, 200, 300, 0, ...|[地铁入站, 地铁出站, 地铁出站...|[地铁一号线, 地铁一号线, 地铁...|  [宝安中心, 桃园, 桃园, 桃园...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[268030115, 26802...|   13|2018-09-01 00:00:00|
|CFAFIDIIB|[2018-09-01 10:37...|[0, 0, 200, 300, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁一号线, 地铁一号线, 地铁...|  [罗湖站, 罗湖站, 大剧院站, ...|[0, 0, 0, 0, 0, 0...|[0, 0, 190, 285, ...|[268001110, 26800...|   12|2018-09-01 00:00:00|
|HHAAABHBH|[2018-09-01 07:43...|[0, 0, 700, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁十一号线, 地铁五号线, 地...|  [桥头站, 翻身, 洪浪北, 翻身...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[241022112, 26301...|   12|2018-09-01 00:00:00|
|HHAAACHHH|[2018-09-01 10:31...|[0, 0, 200, 300, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁五号线, 地铁五号线, 地铁...|    [民治, 长龙, 五和, 杨美, ...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[263025114, 26303...|   12|2018-09-01 00:00:00|
|HHAAACIBC|[2018-09-01 10:54...|[0, 0, 200, 600, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁十一号线, 地铁四号线, 地...|  [南山站, 上塘, 前海湾站, 前...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[241015110, 26201...|   12|2018-09-01 00:00:00|
|HHAAACIBE|[2018-09-01 07:18...|[0, 0, 300, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁五号线, 地铁二号线, 地铁...|  [黄贝岭, 岗厦北, 岗厦北, 莲...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[263037103, 26003...|   12|2018-09-01 00:00:00|
|HHABADHBJ|[2018-09-01 10:26...|[0, 0, 200, 600, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁一号线, 地铁三号线, 地铁...|[会展中心站, 六约, 购物公园站...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[268008121, 26102...|   12|2018-09-01 00:00:00|
|HHJJBEIED|[2018-09-01 08:45...|[0, 0, 200, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁二号线, 地铁二号线, 地铁...|  [岗厦北, 莲花西, 景田, 华强...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[260033111, 26003...|   12|2018-09-01 00:00:00|
|FFJEAIEDG|[2018-09-01 08:56...|[0, 0, 300, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁一号线, 地铁一号线, 地铁...|  [罗湖站, 罗湖站, 科学馆站, ...|[0, 0, 0, 0, 0, 0...|[0, 0, 285, 190, ...|[268001111, 26800...|   11|2018-09-01 00:00:00|
|HHAAJCCGB|[2018-09-01 06:18...|[0, 0, 200, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁五号线, 地铁五号线, 地铁...|  [黄贝岭, 黄贝岭, 黄贝岭, 黄...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[263037102, 26303...|   11|2018-09-01 00:00:00|
|HHAAJFBEI|[2018-09-01 08:23...|[0, 500, 0, 0, 20...|[地铁入站, 地铁出站, 地铁入站...|[地铁三号线, 地铁三号线, 地铁...|   [六约, 晒布, 大剧院, 湖贝,...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[261022113, 26101...|   11|2018-09-01 00:00:00|
|HHAAJFGGE|[2018-09-01 07:03...|[0, 0, 700, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁一号线, 地铁一号线, 地铁...|[大剧院站, 宝安中心, 宝安中心...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[268004103, 26803...|   11|2018-09-01 00:00:00|
|HHJJJDHJG|[2018-09-01 09:17...|[0, 0, 700, 300, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁十一号线, 地铁十一号线, ...|  [福永站, 福永站, 登良, 沙井...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[241021110, 24102...|   11|2018-09-01 00:00:00|
|CGJJABIAF|[2018-09-01 10:26...|[0, 0, 200, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁二号线, 地铁二号线, 地铁...|   [湾厦, 东角头, 水湾, 湾厦,...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[260016115, 26001...|   10|2018-09-01 00:00:00|
|FIABCGCGC|[2018-09-01 07:41...|[0, 200, 700, 0, ...|[地铁入站, 地铁出站, 地铁出站...|[地铁十一号线, 地铁十一号线, ...|  [后亭, 前海湾站, 宝安站, 前...|[0, 0, 0, 0, 0, 0...|[0, 190, 665, 0, ...|[241026125, 24101...|   10|2018-09-01 00:00:00|
|CGJJAFBCH|[2018-09-01 09:20...|[0, 0, 200, 300, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁四号线, 地铁二号线, 地铁...|  [市民中心, 侨香, 市民中心, ...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[268019120, 26002...|   10|2018-09-01 00:00:00|
|HHAAACCJB|[2018-09-01 06:15...|[0, 0, 200, 400, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁十一号线, 地铁十一号线, ...|  [南山站, 南山站, 南山站, 竹...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[241015109, 24101...|   10|2018-09-01 00:00:00|
|BEBJHEBFC|[2018-09-01 07:57...|[0, 0, 300, 300, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁二号线, 地铁四号线, 地铁...|  [安托山, 少年宫, 沙尾, 下梅...|[0, 0, 0, 0, 0, 0...|[0, 0, 150, 150, ...|[260025112, 26802...|   10|2018-09-01 00:00:00|
|HHAAACEEB|[2018-09-01 08:25...|[0, 400, 0, 0, 20...|[地铁入站, 地铁出站, 地铁入站...|[地铁三号线, 地铁九号线, 地铁...|  [晒布, 上梅林, 泥岗, 上梅林...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[261013119, 26702...|   10|2018-09-01 00:00:00|
| BJEJBDDH|[2018-09-01 09:43...|[0, 0, 400, 500, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁十一号线, 地铁三号线, 地...|  [后海站, 草埔, 后海站, 后海站]|[0, 0, 0, 1, 0, 0...|[0, 0, 380, 435, ...|[241014142, 26101...|   10|2018-09-01 00:00:00|
|HHAAAJJAE|[2018-09-01 08:45...|[0, 0, 200, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁二号线, 地铁二号线, 地铁...|  [岗厦北, 莲花西, 景田, 华强...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[260033105, 26003...|   10|2018-09-01 00:00:00|
|HHAAAJJAI|[2018-09-01 07:26...|[0, 0, 400, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁一号线, 地铁十一号线, 地...|  [侨城东站, 宝安站, 前海湾, ...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[268014114, 24101...|   10|2018-09-01 00:00:00|
|HHAAAJJBD|[2018-09-01 10:43...|[400, 0, 0, 500, ...|[地铁出站, 地铁入站, 地铁入站...|[地铁七号线, 地铁一号线, 地铁...|  [沙尾, 竹子林站, 沙尾, 西丽...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[265022134, 26801...|   10|2018-09-01 00:00:00|
|HHAAAJJBI|[2018-09-01 10:29...|[300, 0, 0, 200, ...|[地铁出站, 地铁入站, 地铁入站...|[地铁三号线, 地铁三号线, 地铁...|   [大芬, 丹竹头, 大芬, 六约,...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[261020114, 26102...|   10|2018-09-01 00:00:00|
|HHAAJAAAH|[2018-09-01 10:29...|[500, 0, 0, 200, ...|[地铁出站, 地铁入站, 地铁入站...|[地铁三号线, 地铁三号线, 地铁...|  [塘坑, 通新岭, 通新岭, 老街...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[261023105, 26101...|   10|2018-09-01 00:00:00|
|HHAAJCECF|[2018-09-01 08:47...|[0, 0, 200, 600, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁九号线, 地铁三号线, 地铁...|      [?I岭, 六约, 银湖, ?I岭...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[267022107, 26102...|   10|2018-09-01 00:00:00|
|HHAAJFEFD|[2018-09-01 10:50...|[500, 0, 0, 200, ...|[地铁出站, 地铁入站, 地铁入站...|[地铁三号线, 地铁二号线, 地铁...|    [六约, 景田, 景田, 景田, ...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[261022102, 26002...|   10|2018-09-01 00:00:00|
|HHAAJFGHH|[2018-09-01 10:54...|[0, 0, 200, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁十一号线, 地铁十一号线, ...|  [南山站, 前海湾站, 南山站, ...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[241015111, 24101...|   10|2018-09-01 00:00:00|
|HHABADHBF|[2018-09-01 06:56...|[0, 0, 600, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁五号线, 地铁一号线, 地铁...|  [五和, 前海湾, 前海湾, 前海...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[263026112, 26802...|   10|2018-09-01 00:00:00|
|HHABJFCCH|[2018-09-01 10:42...|[0, 0, 200, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁二号线, 地铁二号线, 地铁...|  [莲花西, 岗厦北, 景田, 景田...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[260030105, 26003...|   10|2018-09-01 00:00:00|
|HHJJAEIFJ|[2018-09-01 08:57...|[200, 0, 0, 200, ...|[地铁出站, 地铁入站, 地铁入站...|[地铁五号线, 地铁五号线, 地铁...|    [兴东, 兴东, 兴东, 兴东, ...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[263018113, 26301...|   10|2018-09-01 00:00:00|
|HHJJAFIBJ|[2018-09-01 08:41...|[0, 0, 200, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁五号线, 地铁五号线, 地铁...|   [西丽, 大学城, 西丽, 西丽,...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[263020121, 26302...|   10|2018-09-01 00:00:00|
|HHJJBACIF|[2018-09-01 10:29...|[300, 500, 0, 0, ...|[地铁出站, 地铁出站, 地铁入站...|[地铁七号线, 地铁五号线, 地铁...|    [农林, 西丽, 农林, 西丽, ...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[265019102, 26302...|   10|2018-09-01 00:00:00|
|HHJJBAJEE|[2018-09-01 10:00...|[0, 0, 200, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁一号线, 地铁一号线, 地铁...|[侨城东站, 侨城东站, 竹子林站...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[268014101, 26801...|   10|2018-09-01 00:00:00|
|HHJJBHADH|[2018-09-01 09:08...|[0, 200, 400, 0, ...|[地铁入站, 地铁出站, 地铁出站...|[地铁三号线, 地铁一号线, 地铁...|  [老街, 竹子林站, 竹子林站, ...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[261012138, 26801...|   10|2018-09-01 00:00:00|
|HHJJJDFHB|[2018-09-01 06:17...|[0, 0, 200, 800, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁二号线, 地铁二号线, 地铁...|   [赤湾, 蛇口港, 水湾, 龙胜,...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[260011105, 26001...|   10|2018-09-01 00:00:00|
|HHJJJHGCC|[2018-09-01 06:22...|[0, 0, 200, 300, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁一号线, 地铁一号线, 地铁...|  [前海湾, 前海湾, 前海湾, 前...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[268028140, 26802...|   10|2018-09-01 00:00:00|
|CBBJDIFCB|[2018-09-01 07:43...|[0, 300, 0, 0, 30...|[地铁入站, 地铁出站, 地铁入站...|[地铁九号线, 地铁九号线, 地铁...|  [下梅林, 下梅林, 下梅林, 下...|[0, 0, 0, 0, 0, 0...|[0, 285, 0, 0, 28...|[267019109, 26701...|    9|2018-09-01 00:00:00|
|HHAAABCFF|[2018-09-01 10:03...|[0, 300, 0, 200, ...|[地铁入站, 地铁出站, 地铁入站...|[地铁三号线, 地铁三号线, 地铁...|  [六约, 木棉湾, 木棉湾, 布吉...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[261022109, 26101...|    9|2018-09-01 00:00:00|
|CFBCJAEIE|[2018-09-01 06:40...|[0, 0, 200, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁四号线, 地铁七号线, 地铁...|  [福民, 皇岗口岸, 福田口岸, ...|[0, 0, 0, 0, 0, 0...|[0, 0, 190, 190, ...|[268018116, 26502...|    9|2018-09-01 00:00:00|
|HHAAACAAE|[2018-09-01 10:25...|[0, 0, 200, 300, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁一号线, 地铁一号线, 地铁...|          [竹子林站, 会展中心站]|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[268008152, 26801...|    9|2018-09-01 00:00:00|
|CFBIHFFAC|[2018-09-01 08:45...|[200, 0, 0, 400, ...|[地铁出站, 地铁入站, 地铁入站...|[地铁四号线, 地铁四号线, 地铁...|  [福田口岸, 民乐, 皇岗口岸, ...|[0, 0, 0, 1, 0, 0...|[190, 0, 0, 340, ...|[268017117, 26201...|    9|2018-09-01 00:00:00|
|HHAAACAEJ|[2018-09-01 10:28...|[200, 0, 0, 200, ...|[地铁出站, 地铁入站, 地铁入站...|[地铁五号线, 地铁五号线, 地铁...|  [长岭陂, 长岭陂, 长岭陂, 长...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[263023116, 26302...|    9|2018-09-01 00:00:00|
|CGJJABIGA|[2018-09-01 09:01...|[0, 200, 200, 0, ...|[地铁入站, 地铁出站, 地铁出站...|[地铁九号线, 地铁九号线, 地铁...|   [红岭, 红岭, 红岭南, 红岭,...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[267027122, 26702...|    9|2018-09-01 00:00:00|
|HHAAACAIJ|[2018-09-01 08:59...|[200, 200, 0, 0, ...|[地铁出站, 地铁出站, 地铁入站...|[地铁五号线, 地铁五号线, 地铁...|  [长岭陂, 长岭陂, 长岭陂, 长...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[263023115, 26302...|    9|2018-09-01 00:00:00|
|CGJJAFBAC|[2018-09-01 10:47...|[0, 0, 200, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁五号线, 地铁五号线, 地铁...|   [民治, 深圳北, 五和, 民治,...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[263025107, 26302...|    9|2018-09-01 00:00:00|
|HHAAACBFE|[2018-09-01 07:35...|[0, 0, 500, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁四号线, 地铁一号线, 地铁...|[竹子林站, 竹子林站, 竹子林站...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[262017112, 26801...|    9|2018-09-01 00:00:00|
|FFFHEBGGI|[2018-09-01 08:49...|[400, 0, 0, 400, ...|[地铁出站, 地铁入站, 地铁入站...|[地铁三号线, 地铁三号线, 地铁...|  [华新, 通新岭, 深圳北站, 深...|[0, 0, 0, 0, 0, 0...|[380, 0, 0, 380, ...|[261009103, 26101...|    9|2018-09-01 00:00:00|
|HHAAJACAI|[2018-09-01 10:32...|[200, 0, 0, 700, ...|[地铁出站, 地铁入站, 地铁入站...|[地铁一号线, 地铁三号线, 地铁...|  [竹子林站, 六约, 竹子林站, ...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[268013106, 26102...|    9|2018-09-01 00:00:00|
|HHAAJBDAH|[2018-09-01 07:43...|[0, 0, 200, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁九号线, 地铁九号线, 地铁...|    [文锦, 文锦, 文锦, 文锦, ...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[267032111, 26703...|    9|2018-09-01 00:00:00|
|HHAAJBEFE|[2018-09-01 08:51...|[400, 0, 0, 200, ...|[地铁出站, 地铁入站, 地铁入站...|[地铁九号线, 地铁一号线, 地铁...|  [深湾站, 鲤鱼门, 深湾站, 深...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[267012105, 26802...|    9|2018-09-01 00:00:00|
|HHAAJBEGI|[2018-09-01 09:30...|[200, 0, 0, 200, ...|[地铁出站, 地铁入站, 地铁入站...|[地铁九号线, 地铁一号线, 地铁...|[深圳湾公园, 华侨城站, 深圳湾...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[267013104, 26801...|    9|2018-09-01 00:00:00|
|HHAAJCBBB|[2018-09-01 08:58...|[200, 0, 0, 200, ...|[地铁出站, 地铁入站, 地铁入站...|[地铁三号线, 地铁三号线, 地铁...|  [通新岭, 通新岭, 通新岭, 华...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[261010106, 26101...|    9|2018-09-01 00:00:00|
|HHAAJFBHH|[2018-09-01 08:48...|[700, 0, 0, 200, ...|[地铁出站, 地铁入站, 地铁入站...|[地铁五号线, 地铁三号线, 地铁...|    [西丽, 六约, 西丽, 西丽, ...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[263020108, 26102...|    9|2018-09-01 00:00:00|
|HHAAJFIFE|[2018-09-01 08:07...|[0, 0, 400, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁七号线, 地铁七号线, 地铁...|   [深云, 赤尾, 华强北, 赤尾,...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[265017112, 26502...|    9|2018-09-01 00:00:00|
|HHAAJGBIH|[2018-09-01 08:23...|[200, 200, 0, 0, ...|[地铁出站, 地铁出站, 地铁入站...|[地铁七号线, 地铁三号线, 地铁...|  [黄木岗, 黄木岗, 八卦岭, 八...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[265032101, 26100...|    9|2018-09-01 00:00:00|
|HHAAJGEAF|[2018-09-01 10:22...|[200, 200, 0, 0, ...|[地铁出站, 地铁出站, 地铁入站...|[地铁二号线, 地铁二号线, 地铁...|  [赤湾, 蛇口港, 赤湾, 蛇口港...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[260011104, 26001...|    9|2018-09-01 00:00:00|
|HHABAEFEH|[2018-09-01 09:05...|[300, 0, 0, 800, ...|[地铁出站, 地铁入站, 地铁入站...|[地铁十一号线, 地铁三号线, 地...|  [碧海湾站, 六约, 碧海湾站, ...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[241018122, 26102...|    9|2018-09-01 00:00:00|
|HHABJGHAH|[2018-09-01 10:37...|[200, 400, 0, 0, ...|[地铁出站, 地铁出站, 地铁入站...|[地铁七号线, 地铁七号线, 地铁...|    [深云, 深云, 深云, 深云, ...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[265017101, 26501...|    9|2018-09-01 00:00:00|
|HHACJABIH|[2018-09-01 07:51...|[0, 0, 600, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁三号线, 地铁四号线, 地铁...|    [老街, 龙胜, 龙胜, 龙胜, ...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[261012108, 26201...|    9|2018-09-01 00:00:00|
|HHJAJJGGD|[2018-09-01 08:43...|[0, 200, 200, 200...|[地铁入站, 地铁出站, 地铁出站...|[地铁四号线, 地铁四号线, 地铁...|  [白石龙, 白石龙, 白石龙, 龙...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[262017601, 26201...|    9|2018-09-01 00:00:00|
|HHJJAFGJH|[2018-09-01 09:00...|[200, 0, 0, 200, ...|[地铁出站, 地铁入站, 地铁入站...|[地铁二号线, 地铁二号线, 地铁...|    [深康, 深康, 深康, 深康, ...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[260024101, 26002...|    9|2018-09-01 00:00:00|
|HHJJAHDHG|[2018-09-01 08:43...|[200, 200, 0, 0, ...|[地铁出站, 地铁出站, 地铁入站...|[地铁一号线, 地铁一号线, 地铁...|[世界之窗站, 世界之窗站, 世界...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[268016109, 26801...|    9|2018-09-01 00:00:00|
|HHJJBBDCA|[2018-09-01 09:09...|[0, 0, 300, 0, 0,...|[地铁入站, 地铁入站, 地铁出站...|[地铁九号线, 地铁九号线, 地铁...|    [鹿丹村, 鹿丹村, ?I岭, 民...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[267029102, 26702...|    9|2018-09-01 00:00:00|
|HHJJBDFBD|[2018-09-01 07:43...|[0, 0, 200, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁三号线, 地铁九号线, 地铁...|  [红岭, 鹿丹村, 鹿丹村, 园岭...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[261011109, 26702...|    9|2018-09-01 00:00:00|
|HHJJJHBBI|[2018-09-01 09:00...|[200, 300, 0, 0, ...|[地铁出站, 地铁出站, 地铁入站...|[地铁一号线, 地铁一号线, 地铁...|[深圳大学, 竹子林站, 深圳大学...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[268024105, 26801...|    9|2018-09-01 00:00:00|
|CFHBAGHIA|[2018-09-01 09:04...|[0, 0, 200, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁四号线, 地铁四号线, 地铁...|  [民乐, 民乐, 民乐, 深圳北站...|[0, 0, 0, 0, 0, 0...|[0, 0, 190, 190, ...|[262018110, 26201...|    8|2018-09-01 00:00:00|
|FHIJHGAHA|[2018-09-01 09:35...|[0, 0, 500, 500, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁四号线, 地铁一号线, 地铁...|  [少年宫, 大新, 大新, 少年宫...|[0, 0, 0, 0, 0, 0...|[0, 0, 475, 475, ...|[268021108, 26802...|    8|2018-09-01 00:00:00|
|CFHCIJEFA|[2018-09-01 08:47...|[0, 0, 700, 700, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁五号线, 地铁三号线, 地铁...|    [灵芝, 草埔, 草埔, 灵芝, ...|[0, 0, 0, 0, 0, 0...|[0, 0, 665, 665, ...|[263016111, 26101...|    8|2018-09-01 00:00:00|
|FIACGCFGC|[2018-09-01 10:25...|[0, 400, 700, 0, ...|[地铁入站, 地铁出站, 地铁出站...|[地铁五号线, 地铁三号线, 地铁...|    [坂田, 横岗, 西乡, 横岗, ...|[0, 0, 0, 0, 0, 1...|[0, 380, 665, 0, ...|[263027121, 26102...|    8|2018-09-01 00:00:00|
|CFHDJCDDH|[2018-09-01 08:39...|[0, 200, 0, 0, 40...|[地铁入站, 地铁出站, 地铁入站...|[地铁一号线, 地铁一号线, 地铁...|  [白石洲, 宝华, 白石洲, 白石洲]|[0, 0, 0, 0, 0, 0...|[0, 190, 0, 0, 38...|[268022119, 26802...|    8|2018-09-01 00:00:00|
|FIAJAHAJB|[2018-09-01 09:17...|[0, 0, 200, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁一号线, 地铁二号线, 地铁...|  [华强路站, 岗厦北, 岗厦站, ...|[0, 0, 0, 0, 0, 0...|[0, 0, 190, 190, ...|[268006125, 26003...|    8|2018-09-01 00:00:00|
|CGJJACJED|[2018-09-01 09:11...|[0, 0, 200, 400, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁一号线, 地铁二号线, 地铁...|  [岗厦站, 深康, 华强路站, 华...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[268007125, 26002...|    8|2018-09-01 00:00:00|
|FIAJEGDCC|[2018-09-01 10:40...|[0, 0, 200, 500, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁一号线, 地铁一号线, 地铁...|  [宝安中心, 固戍, 西乡, 桃园...|[0, 0, 0, 0, 0, 0...|[0, 0, 190, 475, ...|[268030123, 26803...|    8|2018-09-01 00:00:00|
|CGJJAFBHH|[2018-09-01 07:18...|[0, 0, 200, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁三号线, 地铁三号线, 地铁...|    [布吉, 草埔, 水贝, 水贝, ...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[261018116, 26101...|    8|2018-09-01 00:00:00|
|FIHDGCFFC|[2018-09-01 06:55...|[0, 0, 700, 300, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁一号线, 地铁一号线, 地铁...|[宝安中心, 会展中心站, 罗湖站...|[0, 0, 0, 0, 0, 0...|[0, 0, 665, 285, ...|[268030113, 26800...|    8|2018-09-01 00:00:00|
|CGJJAFJJI|[2018-09-01 09:42...|[0, 0, 200, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁九号线, 地铁九号线, 地铁...|[下沙, 深圳湾公园, 深圳湾公园...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[267014118, 26701...|    8|2018-09-01 00:00:00|
|FIJCCEDJA|[2018-09-01 10:09...|[0, 300, 0, 300, ...|[地铁入站, 地铁出站, 地铁入站...|[地铁一号线, 地铁一号线, 地铁...|  [固戍, 宝安中心, 宝安中心, ...|[0, 0, 0, 0, 0, 0...|[0, 285, 0, 285, ...|[268034106, 26803...|    8|2018-09-01 00:00:00|
|FFFAAHGJC|[2018-09-01 08:54...|[0, 0, 200, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁七号线, 地铁一号线, 地铁...|  [笋岗, 华强路站, 大剧院, 华...|[0, 0, 0, 0, 0, 0...|[0, 0, 190, 190, ...|[265035124, 26800...|    8|2018-09-01 00:00:00|
|HHAAAAFFG|[2018-09-01 10:47...|[200, 0, 0, 200, ...|[地铁出站, 地铁入站, 地铁入站...|[地铁九号线, 地铁九号线, 地铁...|   [梅景, 下梅林, 梅景, 梅景,...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[267018102, 26701...|    8|2018-09-01 00:00:00|
|FFFBJIFJF|[2018-09-01 10:33...|[0, 0, 300, 400, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁四号线, 地铁九号线, 地铁...|   [少年宫, 下沙, 下沙, 华新,...|[0, 0, 0, 0, 0, 0...|[0, 0, 285, 380, ...|[268021119, 26701...|    8|2018-09-01 00:00:00|
|HHAAAAJJH|[2018-09-01 06:17...|[0, 0, 200, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁三号线, 地铁三号线, 地铁...|  [丹竹头, 丹竹头, 丹竹头, 大...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[261021102, 26102...|    8|2018-09-01 00:00:00|
|FFFJDFIGF|[2018-09-01 08:52...|[0, 0, 300, 200, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁一号线, 地铁一号线, 地铁...|  [白石洲, 世界之窗站, 桃园, ...|[0, 0, 0, 0, 0, 0...|[0, 0, 285, 190, ...|[268022120, 26801...|    8|2018-09-01 00:00:00|
|HHAAABHFB|[2018-09-01 08:44...|[700, 0, 0, 200, ...|[地铁出站, 地铁入站, 地铁入站...|[地铁一号线, 地铁一号线, 地铁...|  [大新, 前海湾, 大新, 前海湾...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[268026102, 26802...|    8|2018-09-01 00:00:00|
|FFGBIAFBA|[2018-09-01 07:54...|[0, 0, 500, 300, ...|[地铁入站, 地铁入站, 地铁出站...|[地铁三号线, 地铁三号线, 地铁...|    [大运, 翠竹, 大芬, 草埔, ...|[0, 0, 0, 0, 0, 0...|[0, 0, 475, 285, ...|[261027102, 26101...|    8|2018-09-01 00:00:00|
|HHAAACCEC|[2018-09-01 08:00...|[0, 700, 200, 0, ...|[地铁入站, 地铁出站, 地铁出站...|[地铁一号线, 地铁一号线, 地铁...|  [竹子林站, 机场东, 后瑞, 机...|[0, 0, 0, 0, 0, 0...|[0, 0, 0, 0, 0, 0...|[268013116, 26803...|    8|2018-09-01 00:00:00|
+---------+--------------------+--------------------+--------------------------------+--------------------------------+--------------------------------+--------------------+--------------------+--------------------+-----+-------------------+
only showing top 100 rows

22/05/10 12:33:40 INFO BlockManagerInfo: Removed broadcast_4_piece0 on tbds-10-1-0-50:3316 in memory (size: 31.1 KiB, free: 107.6 MiB)
22/05/10 12:33:40 INFO BlockManagerInfo: Removed broadcast_2_piece0 on tbds-10-1-0-50:3316 in memory (size: 31.1 KiB, free: 107.6 MiB)
22/05/10 12:33:40 INFO BlockManagerInfo: Removed broadcast_2_piece0 on tbds-10-1-0-84:32928 in memory (size: 31.1 KiB, free: 107.6 MiB)
22/05/10 12:33:40 INFO BlockManagerInfo: Removed broadcast_6_piece0 on tbds-10-1-0-84:32928 in memory (size: 12.3 KiB, free: 107.6 MiB)
22/05/10 12:33:40 INFO BlockManagerInfo: Removed broadcast_6_piece0 on tbds-10-1-0-50:3316 in memory (size: 12.3 KiB, free: 107.6 MiB)
22/05/10 12:33:40 INFO BlockManagerInfo: Removed broadcast_3_piece0 on tbds-10-1-0-50:3316 in memory (size: 16.1 KiB, free: 107.6 MiB)
22/05/10 12:33:40 INFO BlockManagerInfo: Removed broadcast_3_piece0 on tbds-10-1-0-84:32928 in memory (size: 16.1 KiB, free: 107.6 MiB)
22/05/10 12:33:40 INFO CodeGenerator: Code generated in 42.702169 ms
22/05/10 12:33:40 INFO SparkContext: Starting job: count at SubwayCardRecordWideApplication.scala:59
22/05/10 12:33:40 INFO DAGScheduler: Got job 2 (count at SubwayCardRecordWideApplication.scala:59) with 1 output partitions
22/05/10 12:33:40 INFO DAGScheduler: Final stage: ResultStage 2 (count at SubwayCardRecordWideApplication.scala:59)
22/05/10 12:33:40 INFO DAGScheduler: Parents of final stage: List()
22/05/10 12:33:40 INFO DAGScheduler: Missing parents: List()
22/05/10 12:33:40 INFO DAGScheduler: Submitting ResultStage 2 (MapPartitionsRDD[20] at count at SubwayCardRecordWideApplication.scala:59), which has no missing parents
22/05/10 12:33:40 INFO MemoryStore: Block broadcast_7 stored as values in memory (estimated size 28.2 KiB, free 106.9 MiB)
22/05/10 12:33:40 INFO MemoryStore: Block broadcast_7_piece0 stored as bytes in memory (estimated size 9.0 KiB, free 106.9 MiB)
22/05/10 12:33:40 INFO BlockManagerInfo: Added broadcast_7_piece0 in memory on tbds-10-1-0-50:3316 (size: 9.0 KiB, free: 107.6 MiB)
22/05/10 12:33:40 INFO SparkContext: Created broadcast 7 from broadcast at DAGScheduler.scala:1388
22/05/10 12:33:40 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 2 (MapPartitionsRDD[20] at count at SubwayCardRecordWideApplication.scala:59) (first 15 tasks are for partitions Vector(0))
22/05/10 12:33:40 INFO YarnScheduler: Adding task set 2.0 with 1 tasks resource profile 0
22/05/10 12:33:40 INFO TaskSetManager: Starting task 0.0 in stage 2.0 (TID 2) (tbds-10-1-0-84, executor 2, partition 0, PROCESS_LOCAL, 11859 bytes) taskResourceAssignments Map()
22/05/10 12:33:40 INFO BlockManagerInfo: Added broadcast_7_piece0 in memory on tbds-10-1-0-84:32928 (size: 9.0 KiB, free: 107.6 MiB)
22/05/10 12:33:41 INFO TaskSetManager: Finished task 0.0 in stage 2.0 (TID 2) in 454 ms on tbds-10-1-0-84 (executor 2) (1/1)
22/05/10 12:33:41 INFO YarnScheduler: Removed TaskSet 2.0, whose tasks have all completed, from pool
22/05/10 12:33:41 INFO DAGScheduler: ResultStage 2 (count at SubwayCardRecordWideApplication.scala:59) finished in 0.496 s
22/05/10 12:33:41 INFO DAGScheduler: Job 2 is finished. Cancelling potential speculative or zombie tasks for this job
22/05/10 12:33:41 INFO YarnScheduler: Killing all running tasks in stage 2: Stage finished
22/05/10 12:33:41 INFO DAGScheduler: Job 2 finished: count at SubwayCardRecordWideApplication.scala:59, took 0.513474 s
22/05/10 12:33:41 INFO SubwayCardRecordWideApplication: =============SubwayCardRecordWideApplication finish ! cardCount = 419186 ===================

 */