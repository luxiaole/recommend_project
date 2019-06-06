package com.didi.hive.udf

import java.util.regex.Pattern


class GetParm {



  def getParamByUrl(url: String, name: String): String = {
    val newUrl: String = url + "&"
    val pattern: String = "(\\?|\\&){1}#{0,1}" + name + "=[\\u4E00-\\u9FA5A-Za-z0-9_(-?\\d+)(\\.\\d+)?]*(&{1})"
    val r: Pattern = Pattern.compile(pattern)
    val m = r.matcher(newUrl)
    if (m.find) {
      //System.out.println(m.group(0))
      m.group(0).split("\\=")(1).replace("&", "")
    }
    else null
  }
}
object GetParm{
  def main(args: Array[String]): Unit = {
    val utils = new HiveUtils
    val a = """/MobileInternet/Music/doMusicCollect?track_artist=%E6%B8%A9%E5%B2%9A&track_id=95742&track_file_path=http%3A%2F%2Fdl.stream.qqmusic.qq.com%2FC400003oF0Pc0ahSx4.m4a%3Fvkey%3DA412336F6737A73F1F866CA8CFC0A6B9947D35B739CC4C6CEC2A1580B3769DE121BD36FFD14596B6CE5ACC2DF7522F61BDC25189B18BCD6D%26guid%3D7982961828%26uin%3D0%26fromtag%3D66&track_name=%E5%B1%8B%E9%A1%B6&userid=3408427&city=%E5%90%88%E8%82%A5%E5%B8%82&province=%E5%AE%89%E5%BE%BD%E7%9C%81&area=%E9%95%BF%E4%B8%B0%E5%8E%BF&longitude=117.142841796875&latitude=32.12981553819444&location=%E5%AE%89%E5%BE%BD%E7%9C%81%E5%90%88%E8%82%A5%E5%B8%82%E9%95%BF%E4%B8%B0%E5%8E%BF%E9%9D%A0%E8%BF%91%E9%99%B6%E6%A5%BC%E5%B0%8F%E5%AD%A6&appid=3&logintoken=ad2353db2dbfb202dbe1607d355056ac&platform=android&appname=MJ&version=3.0.2.072418&serialno=0123456789ABCDEF&kernel_version=3.34.1.1&miudriveserver=&miudrive=3.0.2.072418&car_model=&machine_brand=&car_version=&car_mac=C0:81:35:11:23:B6&imei&resolution=1024x600&company=&network=ios_usb&car_brand=&car_models=&car_detail=&didisign=4F6C8FF3A246911ABBA4208FF87F7769""""
    val str: String = utils.evaluate(a)
    val parm = new GetParm
    println(parm.getParamByUrl(str,"track_name"))
  }
}
