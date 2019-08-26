package com.Tags

import com.utils.{RedisUtils, TagUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 上下文标签
  */

object TagsContext {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("目录不匹配，退出程序")
      sys.exit()
    }

    val Array(inputPath, outputPath) = args

    //  创建上下文
    val conf = new SparkConf().setAppName(this.getClass.getName)
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sQLContext = new SQLContext(sc)

    val log: RDD[(String, String)] = sc.textFile("E:\\= =\\项目\\Spark用户画像分析\\app_dict.txt")
      .map(x => x.split("\t", -1))
      .filter(x => x.length >= 5)
      .map(x => {
        val appid = x(4)
        val appname = x(1)

        (appid, appname)
      })

    val AppDir: Broadcast[collection.Map[String, String]] = sc.broadcast(log.collectAsMap())

//    RedisUtils.write(log)

    //  获取停用词库
    val stopword = sc.textFile("E:\\= =\\项目\\Spark用户画像分析\\stopwords.txt").map((_,0)).collectAsMap()
    val bcstopword = sc.broadcast(stopword)

    //  获取数据
    val df: DataFrame = sQLContext.read.parquet(inputPath)

    df.filter(TagUtils.OneUserId)
      .map(row => {

        //  取出用户Id,用户id
        val userId = TagUtils.getOneUserId(row)

        //  接下来通过row数据  打上所有标签（按照需求）,广告标签
        val adList: List[(String, Int)] = TagsAd.makeTags(row)

        //  App标签
        val Applist: List[(String, Int)] =
          TagsApp.makeTags(row, AppDir)

        //  渠道
        val channellist: List[(String, Int)] =
          TagChannel.makeTags(row)

        //  设备
        val Equipment: List[(String, Int)] =
          TagsEquipment.makeTags(row)

        //  关键字
        val KeyWordList: List[(String, Int)] =
          TagsKeyWord.makeTags(row, bcstopword)

        //  地理位置
        val localList: List[(String, Int)] =
          TagsLocations.makeTags(row)

        (userId, adList ++ Applist
          ++ Equipment ++ KeyWordList ++ localList)
      })
      .reduceByKey((list1, list2) =>
        (list1 ::: list2)
        .groupBy(x => x._1)
        .mapValues(x => x.foldLeft[Int](0)((x, y) => x + y._2))
        .toList
      ).foreach(x => println(x))

  }
}
