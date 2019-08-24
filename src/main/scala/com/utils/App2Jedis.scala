package com.utils

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object App2Jedis {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(this.getClass.getName)
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val dict: RDD[String] = sc.textFile("E:\\= =\\项目\\Spark用户画像分析\\app_dict.txt")

    dict.map(x => x.split("\t", -1))
      .filter(x => x.length >= 5)
      .foreachPartition(arr => {
        val jedis = JedisConnectPool.getConnection()

        arr.foreach(
          arr => {
            jedis.set(arr(4), arr(1))
          }
        )
        jedis.close()
      })
    sc.stop()
  }
}
