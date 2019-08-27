package com.utils

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 测试类
  */
object test11 {
  def main(args: Array[String]): Unit = {
    val conf =new SparkConf().setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val list = List("116.310003,39.991957")
    val rdd = sc.makeRDD(list)
    rdd.map(t=>{
      val arr = t.split(",")
      AmapUtils.getBusinessFromAmap(arr(0).toDouble,arr(1).toDouble)
    }).foreach(println)
  }
}
