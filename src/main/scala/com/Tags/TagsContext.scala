package com.Tags

import com.utils.TagUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 上下文标签
  */

object TagsContext {
  def main(args: Array[String]): Unit = {
//    if(args.length != 2){
//      println("目录不匹配，退出程序")
//      sys.exit()
//    }

    val Array(inputPath, outputPath) = args

    //  创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName)
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sQLContext = new SQLContext(sc)


    //  获取数据
    val df: DataFrame = sQLContext.read.parquet(inputPath)

    df.filter(TagUtils.OneUserId)
      .map(row => {

        //  取出用户Id,用户id
        val userId = TagUtils.getOneUserId(row)

        //  接下来通过row数据  打上所有标签（按照需求）,广告标签
        val adList: List[(String, Int)] = TagsAd.makeTags(row)

        //  App标签
        val Applist: List[(String, Int)] = TagsApp.makeTags(row)

        //  渠道
        val channellist: List[(String, Int)] = TagChannel.makeTags(row)

        //  设备
        val value: List[(String, Int)] = TagsEquipment.makeTags(row)
        (userId, adList, Applist, channellist, value)
      }).foreach(x => println(x))


  }
}