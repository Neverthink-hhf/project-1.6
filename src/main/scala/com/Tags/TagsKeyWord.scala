package com.Tags

import com.utils.Tag
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagsKeyWord extends Tag{
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String, Int)]()

    val row: Row = args(0).asInstanceOf[Row]
    val stopWord: Broadcast[Map[String, String]] = args(1).asInstanceOf[Broadcast[Map[String, String]]]

    val kwds: Array[String] = row.getAs[String]("keywords").split("\\|")

    kwds.filter(word => {
      word.length >= 3 && word.length <= 8 && !stopWord.value.contains(word)
    })
      .foreach(word => list :+= ("K" + word, 1))

    list
  }
}
