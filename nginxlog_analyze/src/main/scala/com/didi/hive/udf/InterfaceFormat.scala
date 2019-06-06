package com.didi.hive.udf

import scala.util.matching.Regex

class InterfaceFormat {
  val pattern= """\/([A-Za-z]+/.*/.*[A-Za-z]+)""".r
  def interface_format(str:String):String={
    val interface: Option[Regex.Match] = pattern.findFirstMatchIn(str)
    val inter: String = interface.get.group(0)
    inter
  }
}


object InterfaceFormat{
  def main(args: Array[String]): Unit = {
    val format = new InterfaceFormat
    println(format.interface_format("""//MobileInternet/index/get_Banner\"""))
  }
}