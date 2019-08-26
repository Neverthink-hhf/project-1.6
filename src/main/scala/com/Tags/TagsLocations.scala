package com.Tags

import com.utils.Tag
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsLocations extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {

    var list = List[(String, Int)]()

    val row: Row = args(0).asInstanceOf[Row]

    val pro: String = row.getAs[String]("provincename")
    val city: String = row.getAs[String]("cityname")

    if(StringUtils.isNotBlank(pro)){
      list :+= ("ZP" + pro, 1)
    }

    if(StringUtils.isNotBlank(city)){
      list :+= ("ZC" + city, 1)
    }
    list
  }
}
