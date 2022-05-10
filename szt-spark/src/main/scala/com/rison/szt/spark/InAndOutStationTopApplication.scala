package com.rison.szt.spark

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

/**
 * @PACKAGE_NAME: com.rison.szt.spark
 * @NAME: InAndOutStationopApplication
 * @USER: Rison
 * @DATE: 2022/5/10 13:02
 * @PROJECT_NAME: iceberg-szt-traffic
 * 地铁进出站TOP 统计
 * */

object InAndOutStationTopApplication extends Logging{
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
    logInfo("========== InAndOutStationTopApplication start ...===============")
    val inSql =
      s"""
        |INSERT OVERWRITE TABLE spark_catalog.szt_db.ads_szt_in_station_top
        |SELECT
        |station,
        |collect_list(deal_date),
        |collect_list(card_no),
        |collect_list(company_name),
        |collect_list(equ_no),
        |count(*) AS count,
        |CAST ('$date' AS TIMESTAMP)
        |FROM spark_catalog.szt_db.dwd_szt_subway_in_data
        |WHERE to_date(close_date) = '$date'
        |GROUP BY station
        |ORDER BY count DESC;
        |""".stripMargin

    sparkSession.sql(inSql)

    val outSql =
      s"""
         |INSERT OVERWRITE TABLE spark_catalog.szt_db.ads_szt_out_station_top
         |SELECT
         |station,
         |collect_list(deal_date),
         |collect_list(card_no),
         |collect_list(company_name),
         |collect_list(equ_no),
         |count(*) AS count,
         |CAST ('$date' AS TIMESTAMP)
         |FROM spark_catalog.szt_db.dwd_szt_subway_out_data
         |WHERE to_date(close_date) = '$date'
         |GROUP BY station
         |ORDER BY count DESC;
         |""".stripMargin

    sparkSession.sql(outSql)

    sparkSession.sql("SELECT * FROM spark_catalog.szt_db.ads_szt_in_station_top").show(10)
    sparkSession.sql("SELECT * FROM spark_catalog.szt_db.ads_szt_out_station_top").show(10)

    logInfo("===========InAndOutStationTopApplication finish !=====================")

    sparkSession.close()
  }
}
/**
/usr/hdp/2.2.0.0-2041/spark/bin/spark-submit  --class com.rison.szt.spark.InAndOutStationTopApplication \
    --master yarn \
    --deploy-mode client \
    --driver-memory 500m \
    --executor-memory 500m \
    --executor-cores 1 \
    --queue default \
    /root/spark-dir/szt-spark.jar
 **/

// 入站 top10
/**
 * +-------+--------------------+--------------------+--------------------------------+--------------------+-----+-------------------+
 * |station|       deal_date_arr|         card_no_arr|                company_name_arr|          equ_no_arr|count|                day|
 * +-------+--------------------+--------------------+--------------------------------+--------------------+-----+-------------------+
 * |   null|[2018-09-01 06:19...|[FHDCDCIFI, CBFIG...|[地铁二号线, 地铁四号线, 地铁...|[260029121, 26201...|33814|2018-09-01 00:00:00|
 * |   五和|[2018-09-01 06:19...|[FFHECHBCF, HJJCG...|[地铁五号线, 地铁五号线, 地铁...|[263026121, 26302...|11359|2018-09-01 00:00:00|
 * |   布吉|[2018-09-01 06:19...|[FHDIHADJI, FHDBF...|[地铁五号线, 地铁五号线, 地铁...|[263032105, 26303...| 9871|2018-09-01 00:00:00|
 * | 丹竹头|[2018-09-01 06:19...|[BIJGDBGJH, FHHFC...|[地铁三号线, 地铁三号线, 地铁...|[261021117, 26102...| 8786|2018-09-01 00:00:00|
 * |   民治|[2018-09-01 06:19...|[FIAFJJECG, FHIIC...|[地铁五号线, 地铁五号线, 地铁...|[263025115, 26302...| 8733|2018-09-01 00:00:00|
 * |   龙华|[2018-09-01 06:19...|[BEAIBIIHG, FIJBE...|[地铁四号线, 地铁四号线, 地铁...|[262012104, 26201...| 7901|2018-09-01 00:00:00|
 * |   清湖|[2018-09-01 06:19...|[FHGCAIJIH, FHHFJ...|[地铁四号线, 地铁四号线, 地铁...|[262011106, 26201...| 7619|2018-09-01 00:00:00|
 * | 下水径|[2018-09-01 06:19...|[FFGBJBHJJ, FHECH...|[地铁五号线, 地铁五号线, 地铁...|[263030117, 26303...| 6697|2018-09-01 00:00:00|
 * |   固戍|[2018-09-01 06:19...|[FHGJBHDBC, FFJDJ...|[地铁一号线, 地铁一号线, 地铁...|[268034105, 26803...| 6631|2018-09-01 00:00:00|
 * | 白石洲|[2018-09-01 06:19...|[FHDJIDGJE, FHHAD...|[地铁一号线, 地铁一号线, 地铁...|[268022119, 26802...| 6019|2018-09-01 00:00:00|
 * +-------+--------------------+--------------------+--------------------------------+--------------------+-----+-------------------+
 * only showing top 10 rows
 */

// 出站 top10
/**
 * +----------+--------------------+--------------------+--------------------------------+--------------------+-----+-------------------+
 * |   station|       deal_date_arr|         card_no_arr|                company_name_arr|          equ_no_arr|count|                day|
 * +----------+--------------------+--------------------+--------------------------------+--------------------+-----+-------------------+
 * |      null|[2018-09-01 06:40...|[FHICGJAAA, CFJAG...|[地铁四号线, 地铁一号线, 地铁...|[262020115, 26800...|37116|2018-09-01 00:00:00|
 * |    深圳北|[2018-09-01 06:46...|[CFBIECGBJ, BIAJG...|[地铁五号线, 地铁五号线, 地铁...|[263024121, 26302...| 8963|2018-09-01 00:00:00|
 * |    罗湖站|[2018-09-01 06:56...|[CGJJAFJCG, CGJJA...|[地铁一号线, 地铁一号线, 地铁...|[268001122, 26800...| 8894|2018-09-01 00:00:00|
 * |  福田口岸|[2018-09-01 06:44...|[CBFHGADCJ, CFHCJ...|[地铁四号线, 地铁四号线, 地铁...|[268017120, 26801...| 6838|2018-09-01 00:00:00|
 * |会展中心站|[2018-09-01 06:45...|[CFCJCCGJA, FHIGI...|[地铁一号线, 地铁一号线, 地铁...|[268008128, 26800...| 5841|2018-09-01 00:00:00|
 * |      老街|[2018-09-01 06:44...|[FIAHDBAJI, FIACG...|[地铁三号线, 地铁三号线, 地铁...|[261012153, 26101...| 5696|2018-09-01 00:00:00|
 * |    华强北|[2018-09-01 06:43...|[FHEACCFEB, CBGAC...|[地铁二号线, 地铁七号线, 地铁...|[260034116, 26503...| 5683|2018-09-01 00:00:00|
 * |    南山站|[2018-09-01 06:42...|[FIFBJBGIF, FHDDF...|[地铁十一号线, 地铁十一号线, ...|[241015116, 24101...| 5424|2018-09-01 00:00:00|
 * |      兴东|[2018-09-01 06:44...|[FHFEIFFFA, FHIHI...|[地铁五号线, 地铁五号线, 地铁...|[263018106, 26301...| 5334|2018-09-01 00:00:00|
 * |      五和|[2018-09-01 06:41...|[FIJBHCHFJ, FFEHD...|[地铁五号线, 地铁五号线, 地铁...|[263026115, 26302...| 5058|2018-09-01 00:00:00|
 * +----------+--------------------+--------------------+--------------------------------+--------------------+-----+-------------------+
 * only showing top 10 rows
 */