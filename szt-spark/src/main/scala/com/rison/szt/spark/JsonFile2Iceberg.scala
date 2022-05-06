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

object JsonFile2Iceberg extends Logging {
  def main(args: Array[String]): Unit = {
    val json_path = "hdfs:///rison/ods/szt-data.json"
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
      .setAppName("JsonFile2Iceberg")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._
    //初始化iceberg ods数据
    logInfo(s"==== jsonFile -> ods start... =====")
    sparkSession
      .read
      .json(json_path)
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
      )
      .withColumn("close_date", col("close_date").cast("timestamp"))
      .writeTo("spark_catalog.szt_db.ods_szt_data").overwritePartitions()
    logInfo(s"==== jsonFile -> ods finish ! =====")


    sparkSession.close()
  }
}
/**
/usr/hdp/2.2.0.0-2041/spark/bin/spark-submit  --class com.rison.szt.spark.JsonFile2Iceberg \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 500m \
    --executor-memory 500m \
    --executor-cores 1 \
    --queue default \
    /root/spark-dir/szt-spark.jar
 **/