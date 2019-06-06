package com.didi.hive.udf

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.hadoop.io.Text
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils

/**
  * 将经纬度转化为地址的udf
  */
class ToAddress extends UDF{
  /**
    * 获取get请求返回结果的方法
    * @param url      请求url
    * @param header   请求头
    * @return
    */
  def getResponse(url: String,header: String = null): String ={
    // 创建 client 实例
    val httpClient = HttpClients.createDefault()
    // 创建 get 实例
    val get = new HttpGet(url)

    if (header != null) {
      // 设置 header
      val json = JSON.parseObject(header)
      json.keySet().toArray.map(_.toString).foreach(key => get.setHeader(key, json.getString(key)))
    }

    // 发送请求
    val response = httpClient.execute(get)
    // 获取返回结果
    EntityUtils.toString(response.getEntity)
  }

  /**
    * 重写evaluate方法，实现转换功能
    * @param lon  经度
    * @param lat  纬度
    * @return   解析后地址
    */
  def evaluate(lon: Double,lat: Double): Text = {

    val url = "https://restapi.amap.com/v3/geocode/regeo?output=json&location="+lon+","+lat+"&key=8e31eed0d7e00dca9f38061b3d2d5794"
    val str: String = getResponse(url)
    val jsonObject: JSONObject = JSON.parseObject(str)
    var str1 = jsonObject
      .getString("regeocode")
    val address: String = JSON.parseObject(str1).getString("formatted_address")
    new Text(address)
  }
}

/*//116.310003,39.991957
object ToAddress{
  def main(args: Array[String]): Unit = {
    val toAddress = new ToAddress
    val text: Text = toAddress.evaluate(116.310003,39.991957)
    print(text)
  }
}*/
