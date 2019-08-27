package com.utils

import com.Tags.TagsBusiness
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object BusinessTest {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val ssc = new SQLContext(sc)

    val df: DataFrame = ssc.read.parquet("C:\\Users\\Administrator\\Desktop\\out")

    df.map(row => {
      val business = TagsBusiness.makeTags(row)
      business
    }).filter(x => x(0) == "").count()
  }
}
