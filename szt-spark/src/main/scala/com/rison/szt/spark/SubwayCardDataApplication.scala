package com.rison.szt.spark

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @PACKAGE_NAME: com.rison.szt.spark
 * @NAME: SubwayCardDataApplication
 * @USER: Rison
 * @DATE: 2022/5/9 00:17
 * @PROJECT_NAME: iceberg-szt-traffic
 * 过滤巴士数据和不在正常地铁运营时间的数据
 **/
object SubwayCardDataApplication extends Logging{
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
    logInfo("======SubwayCardDataApplication start...========")
    val sql =
      s"""
        |INSERT OVERWRITE TABLE spark_catalog.szt_db.dwd_szt_subway_data
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
        |spark_catalog.szt_db.dwd_szt_data
        |WHERE
        |deal_type != '巴士'
        |AND unix_timestamp(deal_date, 'yyyy-MM-dd HH:mm:ss') > unix_timestamp('$date 06:14:00', 'yyyy-MM-dd HH:mm:ss')
        |AND unix_timestamp(deal_date, 'yyyy-MM-dd HH:mm:ss') < unix_timestamp('$date 23:59:00', 'yyyy-MM-dd HH:mm:ss')
        |AND to_date(close_date) = '$date'
        |ORDER BY deal_date;
        |""".stripMargin

    val frame: DataFrame = sparkSession.sql(sql)
    val count: Long = sparkSession.sql("SELECT * FROM spark_catalog.szt_db.dwd_szt_subway_data").count()
    logInfo(s"======SubwayCardDataApplication finish! ======== \n dwd_szt_subway_data countNum = $count")
    sparkSession.close()
  }
}
/**
/usr/hdp/2.2.0.0-2041/spark/bin/spark-submit  --class com.rison.szt.spark.SubwayCardDataApplication \
    --master yarn \
    --deploy-mode client \
    --driver-memory 500m \
    --executor-memory 500m \
    --executor-cores 1 \
    --queue default \
    /root/spark-dir/szt-spark.jar
 **/

/**
 * 22/05/09 01:01:00 INFO DAGScheduler: Job 1 finished: count at SubwayCardDataApplication.scala:62, took 3.227866 s
 * 22/05/09 01:01:00 INFO SubwayCardDataApplication: ======SubwayCardDataApplication finish! ========
 * dwd_szt_subway_data countNum = 851867
 * 22/05/09 01:01:00 INFO AbstractConnector: Stopped Spark@733037{HTTP/1.1, (http/1.1)}{0.0.0.0:4042}
 */

/**
 * spark-sql> SELECT COUNT(*) FROM szt_db.dwd_szt_subway_data;
 * 22/05/09 01:03:01 WARN HiveConf: HiveConf of name hive.mapred.supports.subdirectories does not exist
 * 851867
 * Time taken: 2.787 seconds, Fetched 1 row(s)
 */

/**
 * spark-sql> SELECT * FROM szt_db.dwd_szt_subway_data limit 100;
 * 2018-09-01 06:14:01     2018-09-01 00:00:00     FIJCHEGHJ       0       地铁入站        地铁一号线      NULL    NULL    0       0       268005138
 * 2018-09-01 06:14:01     2018-09-01 00:00:00     HHJAJJGFG       0       地铁入站        地铁四号线      NULL    NULL    0       0       262014114
 * 2018-09-01 06:14:01     2018-09-01 00:00:00     FHDEIDFCI       0       地铁入站        地铁九号线      IGT-109 下梅林  0       0       267019109
 * 2018-09-01 06:14:02     2018-09-01 00:00:00     FFFDAEAHJ       0       地铁入站        地铁五号线      IGT-117 杨美    0       0       263028117
 * 2018-09-01 06:14:02     2018-09-01 00:00:00     FIAJFDCDA       0       地铁入站        地铁三号线      AGM-101 南联    0       0       261031101
 * 2018-09-01 06:14:02     2018-09-01 00:00:00     FFGJEBIGG       0       地铁入站        地铁四号线      AGM-111 上塘    0       0       262014111
 * 2018-09-01 06:14:04     2018-09-01 00:00:00     HHJJJEBBJ       200     地铁出站        地铁一号线      OGT-121         竹子林站        00       268013121
 * 2018-09-01 06:14:04     2018-09-01 00:00:00     FHIBBFCED       0       地铁入站        地铁五号线      IGT-112 洪浪北  0       0       263017112
 * 2018-09-01 06:14:04     2018-09-01 00:00:00     FHDDIEGEC       0       地铁入站        地铁十一号线    AGT-127 沙井    0       0       241025127
 * 2018-09-01 06:14:07     2018-09-01 00:00:00     FIAGHBEHF       0       地铁入站        地铁三号线      AGM-102 南联    0       0       261031102
 * 2018-09-01 06:14:07     2018-09-01 00:00:00     FIBAHEDHA       0       地铁入站        地铁十一号线    AGT-127 南山站  0       0       241015127
 * 2018-09-01 06:14:08     2018-09-01 00:00:00     HHAAJBDBF       0       地铁入站        地铁三号线      NULL    NULL    0       0       261009158
 * 2018-09-01 06:14:12     2018-09-01 00:00:00     FIABCIGFI       0       地铁入站        地铁五号线      NULL    NULL    0       0       263020133
 * 2018-09-01 06:14:12     2018-09-01 00:00:00     HHAAJBEID       0       地铁入站        地铁三号线      AGM-101 大芬    0       0       261020101
 * 2018-09-01 06:14:13     2018-09-01 00:00:00     CBJBJGGDA       0       地铁入站        地铁十一号线    IGT-125 后亭    0       0       241026125
 * 2018-09-01 06:14:14     2018-09-01 00:00:00     CBBBAEGBG       0       地铁入站        地铁三号线      AGM-101 南联    0       0       261031101
 * 2018-09-01 06:14:14     2018-09-01 00:00:00     CFHEFECGD       0       地铁入站        地铁四号线      AGM-111 上塘    0       0       262014111
 * 2018-09-01 06:14:15     2018-09-01 00:00:00     CBIGDJFFF       0       地铁入站        地铁五号线      IGT-118 黄贝岭  0       0       263037118
 * 2018-09-01 06:14:16     2018-09-01 00:00:00     FHDJDBFED       0       地铁入站        地铁一号线      NULL    NULL    0       0       268032128
 * 2018-09-01 06:14:16     2018-09-01 00:00:00     FFHDGAADH       0       地铁入站        地铁三号线      AGM-130 华新    0       0       261009130
 * 2018-09-01 06:14:18     2018-09-01 00:00:00     HHAAJCBFB       0       地铁入站        地铁五号线      IGT-106 长岭陂  0       0       263023106
 * 2018-09-01 06:14:18     2018-09-01 00:00:00     FHHEFCCIF       0       地铁入站        地铁九号线      IGT-109 下梅林  0       0       267019109
 * 2018-09-01 06:14:18     2018-09-01 00:00:00     BEBJBGGCG       0       地铁入站        地铁十一号线    IGT-129 南山站  0       0       241015129
 * 2018-09-01 06:14:18     2018-09-01 00:00:00     FIJCJAIAA       0       地铁入站        地铁四号线      AGM-106 清湖    0       0       262011106
 * 2018-09-01 06:14:19     2018-09-01 00:00:00     CBGEBCEEE       0       地铁入站        地铁五号线      AGT-121 五和    0       0       263026121
 * 2018-09-01 06:14:19     2018-09-01 00:00:00     FFGEBGJFE       0       地铁入站        地铁五号线      IGT-118 黄贝岭  0       0       263037118
 * 2018-09-01 06:14:19     2018-09-01 00:00:00     FHIGAFCED       0       地铁入站        地铁五号线      NULL    NULL    0       0       263020132
 * 2018-09-01 06:14:20     2018-09-01 00:00:00     HHAAJFBIB       0       地铁入站        地铁一号线      AGT-101 前海湾  0       0       268028101
 * 2018-09-01 06:14:21     2018-09-01 00:00:00     FHIHBBFHA       0       地铁入站        地铁三号线      AGM-110 田贝    0       0       261015110
 * 2018-09-01 06:14:21     2018-09-01 00:00:00     BIFABDCHC       0       地铁入站        地铁九号线      IGT-109 下梅林  0       0       267019109
 * 2018-09-01 06:14:22     2018-09-01 00:00:00     HHACJJCBJ       200     地铁出站        地铁四号线      AGT-101         市民中心        00       268019101
 * 2018-09-01 06:14:22     2018-09-01 00:00:00     FFGCEJEGD       0       地铁入站        地铁三号线      AGM-120 福田    0       0       261006120
 * 2018-09-01 06:14:23     2018-09-01 00:00:00     FHFFJCIEG       0       地铁入站        地铁五号线      AGT-113 洪浪北  0       0       263017113
 * 2018-09-01 06:14:23     2018-09-01 00:00:00     FHIJIFICA       0       地铁入站        地铁五号线      OGT-121 西丽    0       0       263020121
 * 2018-09-01 06:14:24     2018-09-01 00:00:00     FFFBJJAEE       0       地铁入站        地铁三号线      AGM-102 晒布    0       0       261013102
 * 2018-09-01 06:14:24     2018-09-01 00:00:00     HHAAJBEHA       0       地铁入站        地铁三号线      AGM-101 木棉湾  0       0       261019101
 * 2018-09-01 06:14:27     2018-09-01 00:00:00     FHEIHGIDE       0       地铁入站        地铁五号线      IGT-114 坂田    0       0       263027114
 * 2018-09-01 06:14:28     2018-09-01 00:00:00     FFAEJIJFF       0       地铁入站        地铁四号线      AGM-104 清湖    0       0       262011104
 * 2018-09-01 06:14:29     2018-09-01 00:00:00     BICACAGFI       0       地铁入站        地铁五号线      IGT-121 坂田    0       0       263027121
 * 2018-09-01 06:14:30     2018-09-01 00:00:00     CFCAADEDI       0       地铁入站        地铁五号线      IGT-115 坂田    0       0       263027115
 * 2018-09-01 06:14:31     2018-09-01 00:00:00     CBABFGJCC       0       地铁入站        地铁九号线      IGT-114 梅景    0       0       267018114
 * 2018-09-01 06:14:31     2018-09-01 00:00:00     BEAFFJCGF       0       地铁入站        地铁五号线      IGT-114 坂田    0       0       263027114
 * 2018-09-01 06:14:32     2018-09-01 00:00:00     HHAAACDJH       0       地铁入站        地铁一号线      AGT-104 固戍    0       0       268034104
 * 2018-09-01 06:14:32     2018-09-01 00:00:00     FHIHJBHEA       0       地铁入站        地铁五号线      OGT-121 西丽    0       0       263020121
 * 2018-09-01 06:14:32     2018-09-01 00:00:00     HJDJDECE        0       地铁入站        地铁三号线      AGM-119 晒布    0       0       261013119
 * 2018-09-01 06:14:34     2018-09-01 00:00:00     CBHCGJDHD       0       地铁入站        地铁四号线      AGM-109 上塘    0       0       262014109
 * 2018-09-01 06:14:35     2018-09-01 00:00:00     BICJHBECE       0       地铁入站        地铁九号线      IGT-107 上梅林  0       0       267021107
 * 2018-09-01 06:14:36     2018-09-01 00:00:00     CFHBBBAHG       0       地铁入站        地铁四号线      NULL    NULL    0       0       262011112
 * 2018-09-01 06:14:37     2018-09-01 00:00:00     HHJJAFFGF       200     地铁出站        地铁九号线      OGT-117 深湾站  0       0       267012117
 * 2018-09-01 06:14:37     2018-09-01 00:00:00     FFECEDJBE       0       地铁入站        地铁五号线      IGT-114 坂田    0       0       263027114
 * 2018-09-01 06:14:38     2018-09-01 00:00:00     CBCJCBHCH       0       地铁入站        地铁九号线      IGT-123 梅村    0       0       267020123
 * 2018-09-01 06:14:39     2018-09-01 00:00:00     CBCJIJEGJ       0       地铁入站        地铁五号线      IGT-117 黄贝岭  0       0       263037117
 * 2018-09-01 06:14:40     2018-09-01 00:00:00     CBJGGGHID       0       地铁入站        地铁一号线      OGT-122 会展中心站      0       0268008122
 * 2018-09-01 06:14:41     2018-09-01 00:00:00     CFBCFGEGA       0       地铁入站        地铁五号线      IGT-107 留仙洞  0       0       263019107
 * 2018-09-01 06:14:41     2018-09-01 00:00:00     CBHBDBFCJ       0       地铁入站        地铁一号线      AGM-113 白石洲  0       0       268022113
 * 2018-09-01 06:14:42     2018-09-01 00:00:00     CBAGHBHEH       0       地铁入站        地铁五号线      IGT-117 黄贝岭  0       0       263037117
 * 2018-09-01 06:14:42     2018-09-01 00:00:00     FHFCEACEH       0       地铁入站        地铁七号线      进AGM33-34      赤尾    0       0265028130
 * 2018-09-01 06:14:42     2018-09-01 00:00:00     FHGHHFDEE       0       地铁入站        地铁五号线      AGT-113 洪浪北  0       0       263017113
 * 2018-09-01 06:14:42     2018-09-01 00:00:00     CBICEFJDH       0       地铁入站        地铁四号线      NULL    NULL    0       0       262011109
 * 2018-09-01 06:14:43     2018-09-01 00:00:00     CFAEBECAA       0       地铁入站        地铁五号线      AGT-110 黄贝岭  0       0       263037110
 * 2018-09-01 06:14:44     2018-09-01 00:00:00     CFHCFJHCD       0       地铁入站        地铁一号线      AGM-112 高新园  0       0       268023112
 * 2018-09-01 06:14:45     2018-09-01 00:00:00     FIJAEGCJG       0       地铁入站        地铁五号线      AGT-115 五和    0       0       263026115
 * 2018-09-01 06:14:45     2018-09-01 00:00:00     FHGDDHJIH       0       地铁入站        地铁三号线      AGM-129 华新    0       0       261009129
 * 2018-09-01 06:14:46     2018-09-01 00:00:00     FFGIIDJDF       0       地铁入站        地铁四号线      NULL    NULL    0       0       262011112
 * 2018-09-01 06:14:47     2018-09-01 00:00:00     FFIJBHCEJ       0       地铁入站        地铁一号线      AGM-121 白石洲  0       0       268022121
 * 2018-09-01 06:14:47     2018-09-01 00:00:00     BIDDECGGG       0       地铁入站        地铁十一号线    IGT-108 马鞍山  0       0       241024108
 * 2018-09-01 06:14:48     2018-09-01 00:00:00     CFBIJADJE       0       地铁入站        地铁七号线      宽AGM18-19      上沙    0       0265021115
 * 2018-09-01 06:14:48     2018-09-01 00:00:00     HHJAJJICH       0       地铁入站        地铁四号线      AGM-601 红山    0       0       262015601
 * 2018-09-01 06:14:49     2018-09-01 00:00:00     CCAECDAHD       0       地铁入站        地铁九号线      IGT-105 上梅林  0       0       267021105
 * 2018-09-01 06:14:50     2018-09-01 00:00:00     FFHDHGDJF       0       地铁入站        地铁四号线      AGM-103 清湖    0       0       262011103
 * 2018-09-01 06:14:50     2018-09-01 00:00:00     GJJJIJGJB       0       地铁入站        地铁三号线      AGM-109 翠竹    0       0       261014109
 * 2018-09-01 06:14:50     2018-09-01 00:00:00     HHACJJAGC       0       地铁入站        地铁四号线      AGM-112 民乐    0       0       262018112
 * 2018-09-01 06:14:51     2018-09-01 00:00:00     FIJIEDIJI       0       地铁入站        地铁一号线      IGT-124 桃园    0       0       268025124
 * 2018-09-01 06:14:51     2018-09-01 00:00:00     FHICHGHDE       0       地铁入站        地铁十一号线    IGT-108 桥头站  0       0       241022108
 * 2018-09-01 06:14:52     2018-09-01 00:00:00     FFJEGEBBI       0       地铁入站        地铁三号线      AGM-112 翠竹    0       0       261014112
 * 2018-09-01 06:14:52     2018-09-01 00:00:00     FIJBEFBIC       0       地铁入站        地铁五号线      AGT-113 洪浪北  0       0       263017113
 * 2018-09-01 06:14:53     2018-09-01 00:00:00     BEAEDIBEF       0       地铁入站        地铁五号线      IGT-117 太安    0       0       263035117
 * 2018-09-01 06:14:53     2018-09-01 00:00:00     FHIEHFHGJ       0       地铁入站        地铁五号线      IGT-111 五和    0       0       263026111
 * 2018-09-01 06:14:54     2018-09-01 00:00:00     FFFHGDCGH       0       地铁入站        地铁九号线      IGT-123 梅村    0       0       267020123
 * 2018-09-01 06:14:55     2018-09-01 00:00:00     FHHFBDAFJ       0       地铁入站        地铁五号线      OGT-121 西丽    0       0       263020121
 * 2018-09-01 06:14:55     2018-09-01 00:00:00     HHAAJBCAI       0       地铁入站        地铁三号线      AGM-131 华新    0       0       261009131
 * 2018-09-01 06:14:56     2018-09-01 00:00:00     FHGCGHIEH       0       地铁入站        地铁二号线      IGT-110 侨香    0       0       260026110
 * 2018-09-01 06:14:56     2018-09-01 00:00:00     FIJDEIEJB       0       地铁入站        地铁九号线      宽AGM-118       梅景    0       0267018118
 * 2018-09-01 06:14:56     2018-09-01 00:00:00     BJJDFDDF        0       地铁入站        地铁五号线      IGT-116 太安    0       0       263035116
 * 2018-09-01 06:14:57     2018-09-01 00:00:00     FHHCEFFJE       0       地铁入站        地铁五号线      AGT-121 五和    0       0       263026121
 * 2018-09-01 06:14:59     2018-09-01 00:00:00     HHJJBCDCI       200     地铁出站        地铁一号线      OGT-103 新安    0       0       268029103
 * 2018-09-01 06:15:00     2018-09-01 00:00:00     FIABIJJHJ       0       地铁入站        地铁十一号线    AGT-118 碧头    0       0       241028118
 * 2018-09-01 06:15:01     2018-09-01 00:00:00     BCFJHAEI        0       地铁入站        地铁一号线      NULL    NULL    0       0       268023123
 * 2018-09-01 06:15:01     2018-09-01 00:00:00     CFCBIDBAA       0       地铁入站        地铁五号线      IGT-114 坂田    0       0       263027114
 * 2018-09-01 06:15:02     2018-09-01 00:00:00     CBCHHJFDE       0       地铁入站        地铁五号线      OGT-119 西丽    0       0       263020119
 * 2018-09-01 06:15:02     2018-09-01 00:00:00     FHHEBAIFC       0       地铁入站        地铁十一号线    IGT-107 碧海湾站        0       0241018107
 * 2018-09-01 06:15:03     2018-09-01 00:00:00     BIDEAIAGJ       0       地铁入站        地铁五号线      IGT-117 太安    0       0       263035117
 * 2018-09-01 06:15:04     2018-09-01 00:00:00     CCJIDBHAA       0       地铁入站        地铁一号线      IGT-118 大新    0       0       268026118
 * 2018-09-01 06:15:04     2018-09-01 00:00:00     FFGAFDGBI       0       地铁入站        地铁四号线      NULL    NULL    0       0       262011109
 * 2018-09-01 06:15:04     2018-09-01 00:00:00     FIJHHEDJF       0       地铁入站        地铁三号线      AGM-112 红岭    0       0       261011112
 * 2018-09-01 06:15:05     2018-09-01 00:00:00     BIJGADJBG       0       地铁入站        地铁一号线      IGT-106 后瑞    0       0       268035106
 * 2018-09-01 06:15:06     2018-09-01 00:00:00     FHDJHEFFA       0       地铁入站        地铁一号线      AGM-119 白石洲  0       0       268022119
 * 2018-09-01 06:15:06     2018-09-01 00:00:00     FFHCIHDGB       0       地铁入站        地铁三号线      AGM-118 南联    0       0       261031118
 * 2018-09-01 06:15:07     2018-09-01 00:00:00     CBCBIAGJI       0       地铁入站        地铁十一号线    AGT-116 松岗    0       0       241027116
 * 2018-09-01 06:15:07     2018-09-01 00:00:00     CCJBGCCIB       0       地铁入站        地铁三号线      AGM-109 翠竹    0       0       261014109
 * Time taken: 0.856 seconds, Fetched 100 row(s)
 */