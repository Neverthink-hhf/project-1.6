package com.Rpt

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

object LocationRpt {
  def main(args: Array[String]): Unit = {

    // 1.判断路径是否正确
    if(args.length != 2){
      println("目录参数不正确，退出程序")
      sys.exit()
    }

    // 2.创建一个集合保存输入和输出的目录
    val Array(inputPath, outputPath) = args
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
     // 3.设置序列化方式，采用Kyro方式，比默认序列化方式高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    // 4.创建执行入口

    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    // 5.获取数据
    val logs: DataFrame = sQLContext.read.parquet(inputPath)

    // 6.数据处理统计各个指标
    logs.map(row => {

      // 6.1.把需要的字段全部取到
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val WinPrice: Int = row.getAs[Int]("WinPrice")
      val adpayment: Int = row.getAs[Int]("adpayment")

      // 6.2.取key值，地域的省市

      val provincename: String = row.getAs[String]("provincename")
      val cityname: String = row.getAs[String]("cityname")

      //  根据指标的不同，需要创建三个对应的方法来处理九个指标


      // 6.3.返回元组
      ((provincename, cityname), )

    })

  }
}
