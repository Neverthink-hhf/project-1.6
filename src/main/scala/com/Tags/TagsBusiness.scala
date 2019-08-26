package com.Tags

import ch.hsr.geohash.GeoHash
import com.utils.{AmapUtils, JedisConnectPool, Tag, Utils2Type}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

/**
  * 商圈标签
  */
object TagsBusiness extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    //  解析参数
    val row: Row = args(0).asInstanceOf[Row]
    val long: String = row.getAs[String]("long")
    val lat: String = row.getAs[String]("lat")
    //  获取经纬度、过滤经纬度
    if(Utils2Type.toDouble(long)>= 73.0 &&
      Utils2Type.toDouble(long)<= 135.0 &&
      Utils2Type.toDouble(lat)>=3.0 &&
      Utils2Type.toDouble(lat)<= 54.0){

      //  先去数据库获取商圈
      val business: String = getBusiness(long.toDouble, lat.toDouble)

      //  判断缓存中是否有此商圈
      if(StringUtils.isNotBlank(business)){

        val lines: Array[String] = business.split(",")
        lines.foreach(x => list:+=(x, 1))
      }
    }
    list
  }

  /**
    * 获取商圈信息
    */
  def getBusiness(long: Double, lat: Double): String = {

    //  转换GeoHash字符串
    val geohash: String = GeoHash
      .geoHashStringWithCharacterPrecision(lat, long, 8)

    //  去数据库查询
    var business: String = redis_queryBusiness(geohash)

    //  判断商圈是否为空
    if(business == null || business.length == 0){

      //  通过经纬度获取商圈
      val str: String = AmapUtils.getBusinessFromAmap(long.toDouble, lat.toDouble)

      //  如果调用高德地图解析商圈，那么需要将此次商圈存入redis
      redis_insertBusiness(geohash, business)

    }
    business
  }

  /**
    *  获取商圈信息
    */

  def redis_queryBusiness(geohash:String):String={
    val jedis = JedisConnectPool.getConnection()
    val business = jedis.get(geohash)
    jedis.close()
    business
  }

  /**
    * 存储商圈到redis
    */

  def redis_insertBusiness(geoHash:String,business:String): Unit ={
    val jedis = JedisConnectPool.getConnection()
    jedis.set(geoHash,business)
    jedis.close()
  }
}
