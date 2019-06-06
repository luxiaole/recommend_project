package com.didi.hive.udf

import java.text.{ParseException, SimpleDateFormat}
import java.util.Locale

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.hadoop.io.Text

/**
  * 转换时间格式
  * dd/MMM/yyyy:HH:mm:ss  转为 yyyy-MM-dd HH:mm:ss
  * 注：MMM为月份的英文缩写
  */
class DateFormat extends UDF with Serializable {
  var inputDate = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
  var outputDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def evaluate(time: Text): Text = {
    var date =""
    if (time == null) return null
    if (StringUtils.isBlank(time.toString)) return null
    val parse = time.toString.replaceAll("\"", "")
    try {
      val parseDate = this.inputDate.parse(parse)
     date = this.outputDate.format(parseDate)
    } catch {
      case e: ParseException =>
        e.printStackTrace()
    }
    new Text(date)
  }

}

