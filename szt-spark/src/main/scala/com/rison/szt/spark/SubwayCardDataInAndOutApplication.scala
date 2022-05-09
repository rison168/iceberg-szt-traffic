package com.rison.szt.spark

import com.rison.szt.spark.SubwayCardDataApplication.logInfo
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * @PACKAGE_NAME: com.rison.szt.spark
 * @NAME: SubwayCardDataInAndOutApplication
 * @USER: Rison
 * @DATE: 2022/5/10 01:24
 * @PROJECT_NAME: iceberg-szt-traffic
 * 地铁刷卡数据 ： 地铁入站数据和地铁出站数据
 * */
object SubwayCardDataInAndOutApplication extends Logging{
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
    logInfo("======SubwayCardDataInAndOutApplication start...========")
    val inSql =
      s"""
        |INSERT OVERWRITE TABLE spark_catalog.szt_db.dwd_szt_subway_in_data
        |SELECT
        |deal_date ,
        |close_date  ,
        |card_no ,
        |deal_value ,
        |deal_type ,
        |company_name ,
        |car_no ,
        |station ,
        |conn_mark ,
        |deal_money ,
        |equ_no
        |FROM spark_catalog.szt_db.dwd_szt_subway_data
        |WHERE to_date(close_date) = '$date' AND deal_type = '地铁入站'
        |ORDER BY deal_date;
        |""".stripMargin

    sparkSession.sql(inSql)
    val outSql =
      s"""
         |INSERT OVERWRITE TABLE spark_catalog.szt_db.dwd_szt_subway_out_data
         |SELECT
         |deal_date ,
         |close_date  ,
         |card_no ,
         |deal_value ,
         |deal_type ,
         |company_name ,
         |car_no ,
         |station ,
         |conn_mark ,
         |deal_money ,
         |equ_no
         |FROM spark_catalog.szt_db.dwd_szt_subway_data
         |WHERE to_date(close_date) = '$date' AND deal_type = '地铁出站'
         |ORDER BY deal_date;
         |""".stripMargin

    sparkSession.sql(outSql)
    val inCount: Long = sparkSession.sql("SELECT * FROM spark_catalog.szt_db.dwd_szt_subway_in_data").count()
    val outCount: Long = sparkSession.sql("SELECT * FROM spark_catalog.szt_db.dwd_szt_subway_out_data").count()
    logInfo(s"=======SubwayCardDataInAndOutApplication finish ! ========= \n inCount = $inCount  \n outCount = $outCount")
    sparkSession.close()
  }
}

/**
/usr/hdp/2.2.0.0-2041/spark/bin/spark-submit  --class com.rison.szt.spark.SubwayCardDataInAndOutApplication \
    --master yarn \
    --deploy-mode client \
    --driver-memory 500m \
    --executor-memory 500m \
    --executor-cores 1 \
    --queue default \
    /root/spark-dir/szt-spark.jar
 **/
/**
22/05/10 01:43:43 INFO DAGScheduler: Job 3 finished: count at SubwayCardDataInAndOutApplication.scala:80, took 3.371711 s
22/05/10 01:43:43 INFO SubwayCardDataInAndOutApplication: =======SubwayCardDataInAndOutApplication finish ! =========
 inCount = 449200
 outCount = 402667
22/05/10 01:43:43 INFO AbstractConnector: Stopped Spark@163a855c{HTTP/1.1, (http/1.1)}{0.0.0.0:4040}
**/

// 出站数据
/**
 * spark-sql> select * from dwd_szt_subway_out_data limit 10;
 * 2018-09-01 06:14:04     2018-09-01 13:00:00     HHJJJEBBJ       200     地铁出站        地铁一号线      OGT-121         竹子林站        00       268013121
 * 2018-09-01 06:14:22     2018-09-01 13:00:00     HHACJJCBJ       200     地铁出站        地铁四号线      AGT-101         市民中心        00       268019101
 * 2018-09-01 06:14:37     2018-09-01 13:00:00     HHJJAFFGF       200     地铁出站        地铁九号线      OGT-117 深湾站  0       0       267012117
 * 2018-09-01 06:14:59     2018-09-01 13:00:00     HHJJBCDCI       200     地铁出站        地铁一号线      OGT-103 新安    0       0       268029103
 * 2018-09-01 06:16:45     2018-09-01 13:00:00     HHAAJICJB       500     地铁出站        地铁九号线      双AGM-116       文锦    0       0267032116
 * 2018-09-01 06:16:50     2018-09-01 13:00:00     HHJJJIGHF       200     地铁出站        地铁九号线      双AGM-116       文锦    0       0267032116
 * 2018-09-01 06:17:06     2018-09-01 13:00:00     HHACJAJDI       200     地铁出站        地铁四号线      AGM-114 红山    0       0       262015114
 * 2018-09-01 06:17:07     2018-09-01 13:00:00     FHHDCGHDJ       200     地铁出站        地铁五号线      OGT-122 黄贝岭  0       190     263037122
 * 2018-09-01 06:17:08     2018-09-01 13:00:00     FHEDBIIBI       200     地铁出站        地铁一号线      AGM-117 白石洲  0       190     268022117
 * 2018-09-01 06:17:24     2018-09-01 13:00:00     HHAAACCJB       200     地铁出站        地铁十一号线    AGT-106 南山站  0       0       241015106
 * Time taken: 0.384 seconds, Fetched 10 row(s)
 * spark-sql>
 */
// 入站数据
/**
 * spark-sql> select * from dwd_szt_subway_in_data limit 10;
 * 22/05/10 01:46:00 WARN HiveConf: HiveConf of name hive.mapred.supports.subdirectories does not exist
 * 2018-09-01 06:14:01     2018-09-01 13:00:00     FIJCHEGHJ       0       地铁入站        地铁一号线      NULL    NULL    0       0       268005138
 * 2018-09-01 06:14:01     2018-09-01 13:00:00     HHJAJJGFG       0       地铁入站        地铁四号线      NULL    NULL    0       0       262014114
 * 2018-09-01 06:14:01     2018-09-01 13:00:00     FHDEIDFCI       0       地铁入站        地铁九号线      IGT-109 下梅林  0       0       267019109
 * 2018-09-01 06:14:02     2018-09-01 13:00:00     FFFDAEAHJ       0       地铁入站        地铁五号线      IGT-117 杨美    0       0       263028117
 * 2018-09-01 06:14:02     2018-09-01 13:00:00     FIAJFDCDA       0       地铁入站        地铁三号线      AGM-101 南联    0       0       261031101
 * 2018-09-01 06:14:02     2018-09-01 13:00:00     FFGJEBIGG       0       地铁入站        地铁四号线      AGM-111 上塘    0       0       262014111
 * 2018-09-01 06:14:04     2018-09-01 13:00:00     FHIBBFCED       0       地铁入站        地铁五号线      IGT-112 洪浪北  0       0       263017112
 * 2018-09-01 06:14:04     2018-09-01 13:00:00     FHDDIEGEC       0       地铁入站        地铁十一号线    AGT-127 沙井    0       0       241025127
 * 2018-09-01 06:14:07     2018-09-01 13:00:00     FIAGHBEHF       0       地铁入站        地铁三号线      AGM-102 南联    0       0       261031102
 * 2018-09-01 06:14:07     2018-09-01 13:00:00     FIBAHEDHA       0       地铁入站        地铁十一号线    AGT-127 南山站  0       0       241015127
 * Time taken: 4.416 seconds, Fetched 10 row(s)
 * spark-sql>
 */
