package com.rison.szt.spark

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
 * @PACKAGE_NAME: com.rison.szt.spark
 * @NAME: JsonFile2Iceberg
 * @USER: Rison
 * @DATE: 2022/5/5 12:20
 * @PROJECT_NAME: iceberg-szt-traffic
 * */

object JsonFile2Iceberg extends Logging{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
//      .set("spark.sql.adaptive.enabled", "true")
//      .set("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
//      .set("spark.sql.catalog.local.type", "hadoop")
//      .set("spark.sql.catalog.local.warehouse", "hdfs:///apps/hive/warehouse")
//      .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
//      .set("spark.sql.catalog.spark_catalog.type", "hive")
//      .set("spark.sql.catalog.spark_catalog.uri", "thrift://tbds-172-16-16-41:9083")
//      .set("spark.sql.parquet.binaryAsString", "true")
//      .set("spark.sql.session.timeZone", "CST")
//      .set("spark.sql.warehouse.dir", "/apps/hive/warehouse")
//      .set("spark.sql.catalog.catalog-name.default-namespace", "default")
//      .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
//      .set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
            .setMaster("local[*]")
      .setAppName("JsonFile2Iceberg")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    //初始化iceberg ods数据
    logInfo(s"==== jsonFile -> ods start... =====")
  sparkSession
      .read
      .json("data\\file\\szt-data.json")
      .select(explode(col("data")).as("data"))
      .select("data.deal_date",
        "data.close_date",
        "data.card_no",
        "data.deal_value",
        "data.deal_type",
        "data.company_name",
        "data.car_no",
        "data.station",
        "data.conn_mark",
        "data.deal_money",
        "data.equ_no"
      ).show(1000)





    logInfo(s"==== jsonFile -> ods finish ! =====")


    sparkSession.close()
  }
}
