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
 * 过滤重复的数据
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
      .setAppName("szt-data ods2dwd")
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
/*
/usr/hdp/2.2.0.0-2041/spark/bin/spark-submit  --class com.rison.szt.spark.IcebergOds2Dwd \
    --master yarn \
    --deploy-mode cluster \
    --driver-memory 500m \
    --executor-memory 500m \
    --executor-cores 1 \
    --queue default \
    /root/spark-dir/szt-spark.jar
 **/
/**
 * 22/05/08 23:10:59 INFO DAGScheduler: Job 2 finished: count at IcebergOds2Dwd.scala:60, took 0.122102 s
 * 22/05/08 23:10:59 INFO IcebergOds2Dwd: =====ods -> dwd finish ! ====
 * odsNum = 1338336
 * dwdNum = 1336988
 * filterNum = 1348
 */
/**
 * spark-sql> SELECT COUNT(*) FROM szt_db.ods_szt_data;
 * 22/05/08 23:12:57 WARN HiveConf: HiveConf of name hive.mapred.supports.subdirectories does not exist
 * 1338336
 * Time taken: 7.198 seconds, Fetched 1 row(s)
 * spark-sql> SELECT COUNT(*) FROM szt_db.dwd_szt_data;
 * 1336988
 * Time taken: 0.355 seconds, Fetched 1 row(s)
 * spark-sql> SELECT * FROM szt_db.dwd_szt_data limit 100;
 * 2018-08-27 20:23:26     2018-09-01 00:00:00     DICADFBFE       200     巴士    东部公共交通    CK020   高快巴士772号   0       0       220002603
 * 2018-08-29 10:51:00     2018-09-01 00:00:00     BEABGJGIH       100     巴士    东部公共交通    08607D  B884    0       50      220008957
 * 2018-08-29 18:32:02     2018-09-01 00:00:00     FFFIFJIHB       200     巴士    东部公共交通    05605D  M219    0       160     220003189
 * 2018-08-29 18:33:43     2018-09-01 00:00:00     FFGABAFIC       200     巴士    东部公共交通    05605D  M219    0       160     220003189
 * 2018-08-29 18:38:42     2018-09-01 00:00:00     BJIGBACG        200     巴士    东部公共交通    05605D  M219    0       160     220003189
 * 2018-08-29 19:05:42     2018-09-01 00:00:00     FHHIFCEGD       200     巴士    东部公共交通    03246D  M361    0       160     220001932
 * 2018-08-29 19:31:38     2018-09-01 00:00:00     HJDCGJHF        300     巴士    东部公共交通    03246D  M361    0       200     220001932
 * 2018-08-29 19:31:45     2018-09-01 00:00:00     FIAGFHDHB       300     巴士    东部公共交通    03246D  M361    0       240     220001932
 * 2018-08-29 19:54:53     2018-09-01 00:00:00     DICDBABGH       200     巴士    东部公共交通    05605D  M219    0       0       220003189
 * 2018-08-29 19:57:02     2018-09-01 00:00:00     CBCDFIACH       200     巴士    东部公共交通    05605D  M219    0       160     220003189
 * 2018-08-29 19:58:13     2018-09-01 00:00:00     CBGBJGIEH       200     巴士    东部公共交通    05605D  M219    1       120     220003189
 * 2018-08-29 19:59:02     2018-09-01 00:00:00     FFHCGIJGG       200     巴士    东部公共交通    05605D  M219    0       160     220003189
 * 2018-08-29 19:59:09     2018-09-01 00:00:00     FFEHBABHE       200     巴士    东部公共交通    05605D  M219    0       160     220003189
 * 2018-08-29 20:05:58     2018-09-01 00:00:00     BEBJEABHC       200     巴士    东部公共交通    05605D  M219    0       100     220003189
 * 2018-08-29 20:05:59     2018-09-01 00:00:00     FFFHIBFFD       200     巴士    东部公共交通    05605D  M219    0       160     220003189
 * 2018-08-29 20:09:10     2018-09-01 00:00:00     CCABBHHCE       200     巴士    东部公共交通    05605D  M219    1       120     220003189
 * 2018-08-29 20:15:44     2018-09-01 00:00:00     CBHHJFBHJ       200     巴士    东部公共交通    05605D  M219    0       160     220003189
 * 2018-08-30 08:07:48     2018-09-01 00:00:00     DICDECAAJ       200     巴士    东部公共交通    03246D  M361    0       0       220001932
 * 2018-08-30 08:07:52     2018-09-01 00:00:00     DICDECAAJ       200     巴士    东部公共交通    03246D  M361    0       0       220001932
 * 2018-08-30 09:57:19     2018-09-01 00:00:00     DICDCAAEJ       200     巴士    东部公共交通    temp    归属未知        0       0       220000180
 * 2018-08-30 10:37:11     2018-09-01 00:00:00     DICDHJIDI       200     巴士    东部公共交通    temp    归属未知        0       0       220004056
 * 2018-08-30 10:37:17     2018-09-01 00:00:00     DICDEEHBJ       200     巴士    东部公共交通    temp    归属未知        0       0       220004056
 * 2018-08-30 10:57:34     2018-09-01 00:00:00     DICDCAAEJ       200     巴士    东部公共交通    temp    归属未知        0       0       220000094
 * 2018-08-30 11:15:48     2018-09-01 00:00:00     FFCGGJHAJ       200     巴士    东部公共交通    43247D  839     0       160     220002261
 * 2018-08-30 11:15:52     2018-09-01 00:00:00     CFBIJDABI       200     巴士    东部公共交通    43247D  839     0       160     220002261
 * 2018-08-30 11:15:58     2018-09-01 00:00:00     BIBBHJFEB       200     巴士    东部公共交通    43247D  839     0       160     220002261
 * 2018-08-30 12:27:42     2018-09-01 00:00:00     FHHAIEJEE       200     巴士    东部公共交通    43247D  839     0       160     220002261
 * 2018-08-30 12:36:58     2018-09-01 00:00:00     FHGAEHHCE       200     巴士    东部公共交通    43247D  839     0       160     220002261
 * 2018-08-30 12:42:22     2018-09-01 00:00:00     HJBEAHFE        200     巴士    东部公共交通    43247D  839     0       160     220002261
 * 2018-08-30 12:43:33     2018-09-01 00:00:00     CFAJDDCGF       200     巴士    东部公共交通    43247D  839     0       160     220002261
 * 2018-08-30 12:48:28     2018-09-01 00:00:00     FFEFEIADD       200     巴士    东部公共交通    43247D  839     0       160     220002261
 * 2018-08-30 12:48:48     2018-09-01 00:00:00     CBBIABEIH       200     巴士    东部公共交通    43247D  839     0       160     220002261
 * 2018-08-30 12:58:03     2018-09-01 00:00:00     FFHCCHCHE       400     巴士    东部公共交通    43247D  839     0       315     220002261
 * 2018-08-30 13:01:20     2018-09-01 00:00:00     CCJFEECHE       200     巴士    东部公共交通    43247D  839     0       120     220002261
 * 2018-08-30 13:02:42     2018-09-01 00:00:00     FFEGEBCAF       200     巴士    东部公共交通    43247D  839     0       160     220002261
 * 2018-08-30 13:02:47     2018-09-01 00:00:00     FFJFGHBDF       200     巴士    东部公共交通    43247D  839     0       160     220002261
 * 2018-08-30 13:10:10     2018-09-01 00:00:00     CBEBFBFEH       200     巴士    东部公共交通    00750D  963     0       160     220003372
 * 2018-08-30 13:12:51     2018-09-01 00:00:00     FIAGFDAIF       500     巴士    东部公共交通    43247D  839     0       350     220002261
 * 2018-08-30 13:13:02     2018-09-01 00:00:00     BEBJACAGB       300     巴士    东部公共交通    43247D  839     0       150     220002261
 * 2018-08-30 13:16:11     2018-09-01 00:00:00     BIJDIFADG       200     巴士    东部公共交通    43247D  839     0       120     220002261
 * 2018-08-30 13:16:32     2018-09-01 00:00:00     CCJBFCBEF       400     巴士    东部公共交通    43247D  839     0       275     220002261
 * 2018-08-30 13:17:04     2018-09-01 00:00:00     FHHJDHHEA       200     巴士    东部公共交通    00750D  963     0       160     220003372
 * 2018-08-30 13:20:30     2018-09-01 00:00:00     CCACFIFCD       300     巴士    东部公共交通    43247D  839     0       200     220002261
 * 2018-08-30 13:21:35     2018-09-01 00:00:00     CBBBHCHAJ       400     巴士    东部公共交通    43247D  839     0       275     220002261
 * 2018-08-30 13:21:57     2018-09-01 00:00:00     FFGBEGCEF       200     巴士    东部公共交通    43247D  839     0       120     220002261
 * 2018-08-30 13:22:10     2018-09-01 00:00:00     BEBAFFIBI       400     巴士    东部公共交通    43247D  839     0       200     220002261
 * 2018-08-30 13:26:21     2018-09-01 00:00:00     CBGJJIJJC       300     巴士    东部公共交通    43247D  839     0       200     220002261
 * 2018-08-30 13:26:44     2018-09-01 00:00:00     CBCFEIIGB       400     巴士    东部公共交通    43247D  839     0       275     220002261
 * 2018-08-30 13:28:45     2018-09-01 00:00:00     CFFJFCDJB       300     巴士    东部公共交通    43247D  839     0       200     220002261
 * 2018-08-30 13:28:52     2018-09-01 00:00:00     FHCJJGEJJ       300     巴士    东部公共交通    43247D  839     0       200     220002261
 * 2018-08-30 13:28:54     2018-09-01 00:00:00     BEBJBCHHB       300     巴士    东部公共交通    43247D  839     0       150     220002261
 * 2018-08-30 13:47:34     2018-09-01 00:00:00     FFFGIHBFC       200     巴士    东部公共交通    43247D  839     0       120     220002261
 * 2018-08-30 13:53:23     2018-09-01 00:00:00     BAAAJAGAA       200     巴士    东部公共交通    43247D  839     0       0       220002261
 * 2018-08-30 13:53:27     2018-09-01 00:00:00     BEBABCGEC       200     巴士    东部公共交通    43247D  839     0       100     220002261
 * 2018-08-30 13:54:54     2018-09-01 00:00:00     CBAGJEBCH       200     巴士    东部公共交通    43247D  839     0       160     220002261
 * 2018-08-30 13:56:05     2018-09-01 00:00:00     CBBCEJJIC       200     巴士    东部公共交通    43247D  839     0       120     220002261
 * 2018-08-30 13:57:55     2018-09-01 00:00:00     BIFAFEGGG       200     巴士    东部公共交通    43247D  839     0       160     220002261
 * 2018-08-30 13:58:00     2018-09-01 00:00:00     FHEIDCJJH       200     巴士    东部公共交通    43247D  839     0       160     220002261
 * 2018-08-30 13:58:52     2018-09-01 00:00:00     FFFDBFCAE       200     巴士    东部公共交通    43247D  839     0       120     220002261
 * 2018-08-30 14:00:19     2018-09-01 00:00:00     BIAFDIBAF       200     巴士    东部公共交通    43247D  839     0       120     220002261
 * 2018-08-30 14:02:31     2018-09-01 00:00:00     FHGHAIEGG       200     巴士    东部公共交通    00750D  963     0       160     220003372
 * 2018-08-30 14:02:33     2018-09-01 00:00:00     FFHCJAIAG       300     巴士    东部公共交通    43247D  839     0       200     220002261
 * 2018-08-30 14:06:24     2018-09-01 00:00:00     FHEJADBFI       200     巴士    东部公共交通    43247D  839     0       160     220002261
 * 2018-08-30 14:06:38     2018-09-01 00:00:00     CFAFIGDID       200     巴士    东部公共交通    43247D  839     0       160     220002261
 * 2018-08-30 14:11:07     2018-09-01 00:00:00     FHIJFDIFB       200     巴士    东部公共交通    43247D  839     0       120     220002261
 * 2018-08-30 14:22:26     2018-09-01 00:00:00     DICAGIEJB       200     巴士    东部公共交通    00750D  963     0       0       220003372
 * 2018-08-30 14:26:56     2018-09-01 00:00:00     FFEJIBHEA       200     巴士    东部公共交通    00750D  963     0       160     220003372
 * 2018-08-30 14:31:35     2018-09-01 00:00:00     CFBGCGADD       200     巴士    东部公共交通    43247D  839     0       160     220002261
 * 2018-08-30 15:16:32     2018-09-01 00:00:00     CBAEIAJFC       200     巴士    东部公共交通    00750D  963     0       160     220003372
 * 2018-08-30 15:18:01     2018-09-01 00:00:00     CBBFGJAEB       200     巴士    东部公共交通    00750D  963     0       160     220003372
 * 2018-08-30 15:18:04     2018-09-01 00:00:00     FHHHBGCAF       200     巴士    东部公共交通    00750D  963     0       160     220003372
 * 2018-08-30 15:28:36     2018-09-01 00:00:00     FHIICBHHH       200     巴士    东部公共交通    00750D  963     1       120     220003372
 * 2018-08-30 15:28:37     2018-09-01 00:00:00     FFHDEFHCF       200     巴士    东部公共交通    00750D  963     0       160     220003372
 * 2018-08-30 16:09:24     2018-09-01 00:00:00     CBDIIAHHD       200     巴士    东部公共交通    00750D  963     1       120     220003372
 * 2018-08-30 16:20:43     2018-09-01 00:00:00     FHHGBADGG       200     巴士    东部公共交通    00750D  963     0       160     220003372
 * 2018-08-30 17:00:40     2018-09-01 00:00:00     FFJEBJBCD       200     巴士    东部公共交通    43247D  839     0       160     220002261
 * 2018-08-30 17:00:43     2018-09-01 00:00:00     BEADHAHDG       200     巴士    东部公共交通    43247D  839     0       100     220002261
 * 2018-08-30 17:33:55     2018-09-01 00:00:00     FIAIBFCCC       200     巴士    东部公共交通    43247D  839     0       120     220002261
 * 2018-08-30 17:37:08     2018-09-01 00:00:00     FHGDGCGFF       200     巴士    东部公共交通    43247D  839     0       160     220002261
 * 2018-08-30 17:37:12     2018-09-01 00:00:00     BEBBBFECG       200     巴士    东部公共交通    43247D  839     0       100     220002261
 * 2018-08-30 17:41:00     2018-09-01 00:00:00     CBGCIBBAJ       200     巴士    东部公共交通    00750D  963     0       160     220003372
 * 2018-08-30 17:44:20     2018-09-01 00:00:00     FIJGAEJCA       200     巴士    东部公共交通    43247D  839     0       160     220002261
 * 2018-08-30 17:48:18     2018-09-01 00:00:00     FHEJBFCJF       200     巴士    东部公共交通    00750D  963     0       160     220003372
 * 2018-08-30 17:48:29     2018-09-01 00:00:00     BIACABCCJ       200     巴士    东部公共交通    00750D  963     1       120     220003372
 * 2018-08-30 17:48:30     2018-09-01 00:00:00     CCJABGEAC       200     巴士    东部公共交通    00750D  963     0       160     220003372
 * 2018-08-30 17:52:12     2018-09-01 00:00:00     BEAGGDGJH       200     巴士    东部公共交通    00750D  963     0       100     220003372
 * 2018-08-30 17:58:43     2018-09-01 00:00:00     CBEIHJGAH       200     巴士    东部公共交通    43247D  839     0       160     220002261
 * 2018-08-30 18:03:46     2018-09-01 00:00:00     BIDFHIGDI       200     巴士    东部公共交通    43247D  839     0       160     220002261
 * 2018-08-30 18:34:33     2018-09-01 00:00:00     FIACCJICI       200     巴士    东部公共交通    00750D  963     1       120     220003372
 * 2018-08-30 19:29:45     2018-09-01 00:00:00     BIDEHHIDE       200     巴士    东部公共交通    00750D  963     0       160     220003372
 * 2018-08-30 19:37:42     2018-09-01 00:00:00     FHEIGDBGB       200     巴士    东部公共交通    00750D  963     1       120     220003372
 * 2018-08-30 19:54:55     2018-09-01 00:00:00     CFJGDDAGI       200     巴士    东部公共交通    00750D  963     1       120     220003372
 * 2018-08-30 19:56:21     2018-09-01 00:00:00     BEBJDBCHG       200     巴士    东部公共交通    00750D  963     0       100     220003372
 * 2018-08-30 19:56:23     2018-09-01 00:00:00     BICHHHAEE       200     巴士    东部公共交通    00750D  963     0       160     220003372
 * 2018-08-30 20:26:57     2018-09-01 00:00:00     FFHCBHDIB       200     巴士    东部公共交通    00750D  963     1       120     220003372
 * 2018-08-30 20:54:30     2018-09-01 00:00:00     CFCACHBFI       200     巴士    东部公共交通    00750D  963     0       160     220003372
 * 2018-08-30 20:59:22     2018-09-01 00:00:00     CBCEBJAEH       200     巴士    东部公共交通    00750D  963     0       160     220003372
 * 2018-08-31 00:17:33     2018-09-01 00:00:00     CCJIHEADE       200     巴士    东部公共交通    02823D  387     0       120     220002624
 * 2018-08-31 00:18:04     2018-09-01 00:00:00     CCJIEECAB       200     巴士    东部公共交通    02823D  387     0       120     220002624
 * 2018-08-31 00:21:37     2018-09-01 00:00:00     CBCHDHBFE       200     巴士    东部公共交通    02823D  387     0       120     220002624
 * Time taken: 0.797 seconds, Fetched 100 row(s)
 * spark-sql>
 */
