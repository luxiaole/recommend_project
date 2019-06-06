package com.didi.hive.udf

import jodd.util.URLDecoder
import org.apache.hadoop.hive.ql.exec.UDF

class HiveUtils extends UDF with Serializable {
  /**
    * 解决参数乱码的udf
    * @param url
    * @return
    */
  def evaluate(url:String):String = {
    //必须强调的是编码方式必须正确，如baidu的是gb2312，而google的是UTF-8
    val keyWord = URLDecoder.decode(url, "utf-8")
    keyWord
  }
}

object HiveUtils{
  def main(args: Array[String]): Unit = {
    val utils = new HiveUtils
    println(utils.evaluate("59.36.132.144 - - [24/May/2019:05:33:45 +0800] \"\\x00\\x9C\\x00\\x01\\x1A+<M\\x00\\x01\\x00\\x00\\x01\\x00M\\x00\\x00\\x00\\x00\\x01\\x00\\x00\\x00\\x01\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\" 400 172 \"-\" \"-\""))
  }
}
