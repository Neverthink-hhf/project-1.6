package com.Tags

import com.utils.{RedisUtils, Tag}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object TagsApp extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String, Int)]()
    var app_name = ""

    //  解析参数
    val row: Row = args(0).asInstanceOf[Row]
    val broadcast: Broadcast[collection.Map[String, String]] = args(1).asInstanceOf[Broadcast[collection.Map[String, String]]]

    // 获取广告Id
    val appid: String = row.getAs[String]("appid")

    //  获取广告类型、广告类型名称

    val appname: String = row.getAs[String]("appname")

    if(appname.equals("其他") || appname.equals("未知")){

//      app_name = RedisUtils.read.hget(RedisUtils.tableName, appid)
      app_name = broadcast.value.getOrElse(appid, null)

      if(StringUtils.isNotBlank(app_name)){
        list:+=("APP" + app_name,1)
      }
    }else{
      app_name = appname
      if(StringUtils.isNotBlank(app_name)){
        list:+=("APP" + app_name,1)
      }
    }

    list
  }
}
