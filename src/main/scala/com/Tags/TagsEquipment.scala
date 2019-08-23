package com.Tags

import com.utils.Tag
import org.apache.spark.sql.Row

object TagsEquipment extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*):List[(String, Int)] = {

    var list = List[(String, Int)]()

    val row = args(0).asInstanceOf[Row]

    val client: Int = row.getAs("client")
    client match {
      case v if v == 1 => list:+=("D00010001", 1)
      case v if v == 2 => list:+=("D00010002", 1)
      case v if v == 3 => list:+=("D00010003", 1)
      case _ => list:+=("D00010004", 1)
    }

    val networkmannername: String = row.getAs("networkmannername")
    networkmannername match {
      case v if v.equals("Wifi") => list:+=("D00020001", 1)
      case v if v.equals("4G") => list:+=("D00020002", 1)
      case v if v.equals("3G") => list:+=("D00020003", 1)
      case v if v.equals("2G") => list:+=("D00020004", 1)
      case _ => list:+=("D00020005", 1)
    }

    val ispname: String = row.getAs("ispname")
    ispname match {
      case v if v.equals("移动") => list:+=("D00030001", 1)
      case v if v.equals("联通") => list:+=("D00030002", 1)
      case v if v.equals("电信") => list:+=("D00030003", 1)
      case _ => list:+=("D00030004", 1)
    }

    list
  }
}
