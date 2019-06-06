package com.didi.hive.udf

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.hive.ql.exec.UDF


/**
  * 根据业务需求统计用户使用功能的自定义方法
  */
class FormatData extends UDF{

  /**
    * 模式匹配，对应的功能模块返回不同的参数
    * @param data   参数内容
    * @return
    */
  def evaluate(data:String):String = {

    val jsonOB: JSONObject = JSON.parseObject(data)
    val module: String = jsonOB.getString("module")
    module match {
        case "playlist" => jsonOB.getString("song_source")+":"+jsonOB.getString("song_name")
        case "radio" => "收听电台:"+jsonOB.getString("album_name")
        case  "voice_search" => "搜索内容："+jsonOB.getString("end_point")
        case  "wechat" => "昵称："+jsonOB.getString("user_name")
        case  "qq_music" => "qq音乐："+jsonOB.getString("music_name")
        case  "video" => "投屏内容："+jsonOB.getString("video_name")
        case  "mobile_music" => jsonOB.getString("song_source")+":"+jsonOB.getString("song_name")
    }
  }

}


object FormatData{
  def main(args: Array[String]): Unit = {
    val data = new FormatData
    println(data.evaluate(
      """{"operation":"server","module":"playlist","flag":"start","lyric":"","song_source":"车友歌单","song_name":"光的方向","action":"listen","album_pic_url":"https:\/\/srctest.didi365.com\/didi365\/Upload\/Miudrive\/musicSingerPhoto\/20180914\/5b9b5c75c423b_440x440.jpg","listen_time":"4","song_time":"","track_id":"213315513","playlist_name":""}
        """))
  }
}