package com.rison.szt.spark

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * @PACKAGE_NAME: com.rison.szt.spark
 * @NAME: IcebergOds2Dwd
 * @USER: Rison
 * @DATE: 2022/5/6 17:30
 * @PROJECT_NAME: iceberg-szt-traffic
 * */

object IcebergOds2Dwd extends Logging{
  def main(args: Array[String]): Unit = {
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
      .setAppName("szt-data-ods2szt-data-dwd")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    logInfo("=====ods -> dwd start...=====")
    //开始读ods数据
    val sql =
      """
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
        |FROM
        |spark_catalog.szt_db.ods_szt_data
        |""".stripMargin
    val oDSFrame = sparkSession.sql(sql)

    //开始插入数据
    oDSFrame.distinct().writeTo("spark_catalog.szt_db.dwd_szt_data").overwritePartitions()
    //统计前后数据数量
    val oDSCount = oDSFrame.count()
    val dWDCount = sparkSession.sql("SELECT * FROM spark_catalog.szt_db.dwd_szt_data").count()
    val filterNum: Long = oDSCount - dWDCount
    logInfo(s"=====ods -> dwd finish ! ==== \n odsNum = $oDSCount \n dwdNum = $dWDCount \n filterNum = $filterNum")

    sparkSession.close()
  }

}
