package com.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.{Jedis, JedisPool}

object RedisUtils {

    def JedisData() :Jedis = {
      val conf = new SparkConf().setAppName(this.getClass.getName)
        .setMaster("local[*]")

      val sc = new SparkContext(conf)

      val sQLContext = new SQLContext(sc)

      val logs: RDD[String] = sc.textFile("E:\\= =\\项目\\Spark用户画像分析\\app_dict.txt")

      val log: RDD[(String, String)] = logs.map(x => x.split("\\s", x.length))
        .filter(x => x.length >= 5)
        .map(arr => {
          val appid = arr(4)
          val appname = arr(1)
          (appid, appname)
        })

      val word: Array[String] = log.toString().mkString(",").split(",")

      println(word.length)


//      val pool = new JedisPool("192.168.11.66")
//      val jedis = pool.getResource
//      jedis.mset("appid", word(0), "appname", word(1))




    }
}
