package com.utils

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable.ListBuffer


object AmapUtils {


  //  获取高德地图商圈信息

  def getBusinessFromAmap(long: Double, lat: Double): String = {

    val location = long + "," + lat

    val urlStr = "https://restapi.amap.com/v3/geocode/regeo?" +
      "key=ac807ae142f784fb4ab664889fe9ffb2&location=" + location


    //  调用请求
    val jsonStr = HttpUtil.get(urlStr)

    //  解析json串
    val jsonparse: JSONObject = JSON.parseObject(jsonStr)

    //  判断状态是否成功
    val status: Int = jsonparse.getIntValue("status")
    if(status == 0) return ""

    //  接下来解析内部json串
    //    判断每个Key的Value都不能为空
    val regeocodesJson: JSONObject = jsonparse.getJSONObject("regeocodes")
    if(regeocodesJson == null || regeocodesJson.keySet().isEmpty) return ""

    val addressComponentJson: JSONObject = regeocodesJson.getJSONObject("addressComponent")
    if(addressComponentJson == null || addressComponentJson.keySet().isEmpty) return ""

    val businessAreasArray: JSONArray = addressComponentJson.getJSONArray("businessAreas")
    if(businessAreasArray == null || businessAreasArray.isEmpty) return null

    //  创建集合 保存数据
    val buffer: ListBuffer[String] = collection.mutable.ListBuffer[String]()

    //  循环输出
    for(item <- businessAreasArray.toArray){
      if(item.isInstanceOf[JSONObject]){
        val json = item.asInstanceOf[JSONObject]
        buffer.append(json.getString("name"))
      }
    }
    buffer.mkString(",")
  }
}
