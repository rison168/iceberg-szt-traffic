package com.rison.szt.spark

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * @PACKAGE_NAME: com.rison.szt.spark
 * @NAME: SubwayCardConsumptionTopApplication
 * @USER: Rison
 * @DATE: 2022/5/15 12:17
 * @PROJECT_NAME: iceberg-szt-traffic
 *                每天地铁刷卡消费排行
 * */

object SubwayCardConsumptionTopApplication extends Logging {
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
    logInfo("=======================SubwayCardConsumptionTopApplication start...============================")
    val sql =
      s"""
         |INSERT OVERWRITE TABLE spark_catalog.szt_db.ads_szt_card_deal_top
         |SELECT
         |a.card_no,
         |a.deal_date_arr,
         |b.deal_value_sum,
         |a.company_name_arr,
         |a.station_arr,
         |a.conn_mark_arr,
         |c.deal_money_sum,
         |a.equ_no_arr,
         |a.count,
         |CAST ('$date' AS  TIMESTAMP)
         |FROM spark_catalog.szt_db.dws_szt_card_record_wide AS a
         |LEFT JOIN (SELECT
         |         card_no,
         |         SUM(deal_v) AS deal_value_sum
         |     FROM spark_catalog.szt_db.dws_szt_card_record_wide
         |     LATERAL VIEW explode(deal_value_arr) as deal_v GROUP BY card_no) AS b  ON a.card_no = b.card_no
         |LEFT JOIN (SELECT
         |          card_no,
         |          SUM(deal_m) AS deal_money_sum
         |      FROM spark_catalog.szt_db.dws_szt_card_record_wide
         |      LATERAL VIEW explode(deal_money_arr) as deal_m GROUP BY card_no) AS c ON a.card_no = c.card_no
         |WHERE to_date(a.day) = '$date'
         |ORDER BY b.deal_value_sum DESC
         |;
         |""".stripMargin
    sparkSession.sql(sql)
    logInfo("========================SubwayCardConsumptionTopApplication finish ===========================")
    sparkSession.sql("SELECT card_no, deal_value_sum, deal_money_sum, count FROM spark_catalog.szt_db.ads_szt_card_deal_top LIMIT 10").show
    logInfo("===consume top 10 =====")
    sparkSession.close()
  }
}

/**
/usr/hdp/2.2.0.0-2041/spark/bin/spark-submit  --class com.rison.szt.spark.SubwayCardConsumptionTopApplication \
--master yarn \
--deploy-mode client \
--driver-memory 500m \
--executor-memory 500m \
--executor-cores 1 \
--queue default \
/root/spark-dir/szt-spark.jar
 * */

/**
 * +---------+--------------+--------------+-----+
 * |  card_no|deal_value_sum|deal_money_sum|count|
 * +---------+--------------+--------------+-----+
 * |FHIGEIEDB|        4800.0|        4800.0|    4|
 * |FHIGHHBFA|        4200.0|        4200.0|    4|
 * |FFGEDJEGD|        4200.0|        4200.0|    4|
 * |FIJHADJHB|        4200.0|        4200.0|    4|
 * |FHFGEDDAF|        4200.0|        4200.0|    4|
 * |FIJABIBFD|        3900.0|        3900.0|    4|
 * |BICGEJCGE|        3900.0|        3900.0|    4|
 * |FHICFDCEH|        3600.0|        3600.0|    4|
 * |FHCJJGAGG|        3600.0|        3600.0|    4|
 * |BIBAIHJDI|        3600.0|        3600.0|    4|
 * +---------+--------------+--------------+-----+
 *
 *
 *
 * 22/05/19 10:07:43 INFO SubwayCardConsumptionTopApplication: ===consume top 10 =====
 */