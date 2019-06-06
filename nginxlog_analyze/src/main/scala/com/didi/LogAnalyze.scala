package com.didi




import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.matching.Regex


object LogAnalyze {
  val PATTERN = """([^ ]*) ([^ ]*) ([^ ]*) (\[.*\]) (\".*?\") (-|[0-9]*) (-|[0-9]*) (\".*?\") (\".*?\")""".r
  //设置需要统计的页面
  val pages = new mutable.HashSet[String]()
  pages.add("/MobileInternet/index/uploadData")
  pages.add("/MobileInternet/index/version")
  pages.add("/MobileInternet/Creategroup/check")
  pages.add("/MobileInternet/Creategroup/create")
  pages.add("/MobileInternet/Creategroup/join")
  pages.add("/MobileInternet/Creategroup/signout")
  pages.add("/MobileInternet/index/getBanner")
  pages.add("/MobileInternet/index/mileageUpload")
  pages.add("/MobileInternet/index/msxiaoice")
  pages.add("/MobileInternet/index/sn")
  pages.add("/MobileInternet/index/uploadLocus")
  pages.add("/MobileInternet/index/uploadUseLog")
  pages.add("/MobileInternet/index/version")
  pages.add("/MobileInternet/Music/collectionList")
  pages.add("/MobileInternet/Music/doMusicCollect")
  pages.add("/MobileInternet/Music/menuMusicList")
  pages.add("/MobileInternet/Music/menuRecList")
  pages.add("/MobileInternet/Radio/collect_radio")
  pages.add("/MobileInternet/Radio/collectRadio_list")
  pages.add("/MobileInternet/Radio/is_collect")
  pages.add("/MobileInternet/Service/canLink")
  pages.add("/MobileInternet/service/checkCode")
  pages.add("/MobileInternet/service/getCode")
  pages.add("/MobileInternet/service/getIcon")
  pages.add("/MobileInternet/user/delCollectRecord")
  pages.add("/MobileInternet/user/getPreset")
  pages.add("/MobileInternet/user/getSearchList")
  pages.add("/MobileInternet/user/getUsers")
  pages.add("/MobileInternet/user/putCollectRecord")
  pages.add("/MobileInternet/user/putSearchRecord")
  pages.add("/MobileInternet/user/welcome")
  pages.add("/MobileInternet/Voice/order_json")


  def parse(line: String): KPI2 = {
    //转码

   /* val utils = new HiveUtils
    val t_line: String = utils.evaluate(line)*/
   val res: Option[Regex.Match] = PATTERN.findFirstMatchIn(line)
    if (res.isEmpty) {
      throw new RuntimeException("Cannot parse log line: " + line)
    }
    val value: Regex.Match = res.get

    KPI2(value.group(1),value.group(2),value.group(3),value.group(4),value.group(5),value.group(6),value.group(7),value.group(8),value.group(9))

}

 /* def filterLog(line: String): KPI = {
    val kpi:KPI = parse(line)
    kpi
  }*/

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("LogAnalyze").setMaster("local[*]")
    val sc: SparkContext = SparkContext.getOrCreate(conf)
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val logRDD = sc.textFile("hdfs://master:8022/didi365/*").cache()
    //val logRDD = sc.textFile("D:\\workspace\\recommend_project\\nginxlog_analyze\\src\\main\\resources\\*.log").cache()
    import spark.implicits._
    for{page <- pages} yield {
      logRDD.map(x => {
        /**
          * 封装并过滤数据
          */
        parse(x)
      }).filter(_.request.contains(page)).toDF.repartition(1)
        .write.mode(SaveMode.Append)
        //.orc("D:\\workspace\\recommend_project\\nginxlog_analyze\\src\\main\\resources\\out")
        .orc("hdfs://master:8022/user/hive/ods/didi_log_ods/nginx_orc_ods")
    }

    sc.stop()
    spark.stop()

  }


}

case class KPI2(
                ipaddress:String,
                identity:String,
                user:String,
                time:String,
                request:String,
                status:String,
                size:String,
                referer:String,
                agent:String
              ) extends Serializable