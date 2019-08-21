package com.Terminal

import java.util.Properties

import com.typesafe.config.ConfigFactory
import com.utils.RptUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object Network {
  def main(args: Array[String]): Unit = {

    // 判断路径是否正确
    if(args.length != 2){
      println("目录参数不正确，退出程序")
      sys.exit()
    }

    // 创建一个集合来保存输入和输出目录
    val Array(inputPath, outputPath) = args

    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local[*]")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sc = new SparkContext(conf)
    val sQLcontext = new SQLContext(sc)

    val logs: DataFrame = sQLcontext.read.parquet(inputPath)

    val tup: RDD[(String, List[Double])] = logs.map(row => {

      // 把需要的字段全部取到
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode: Int = row.getAs[Int]("processnode")
      val iseffective: Int = row.getAs[Int]("iseffective")
      val isbilling: Int = row.getAs[Int]("isbilling")
      val isbid: Int = row.getAs[Int]("isbid")
      val iswin: Int = row.getAs[Int]("iswin")
      val adorderid: Int = row.getAs[Int]("adorderid")
      val winprice: Double = row.getAs[Double]("winprice")
      val adpayment: Double = row.getAs[Double]("adpayment")

      //  需要的网络类型
      val networkmannername: String = row.getAs[String]("networkmannername")
      var networkname = ""
      if (networkmannername == "2G") {
        networkname = networkmannername
      } else if (networkmannername == "3G") {
        networkname = networkmannername
      } else if (networkmannername == "4G") {
        networkname = networkmannername
      }else if (networkmannername == "Wifi") {
        networkname = networkmannername
      } else {
        networkname = "其他"
      }

      //  根据指标的不同，需要创建三个对应的方法来处理九个指标

      //  此方法处理请求数
      val list1: List[Double] = RptUtils.request(requestmode, processnode)

      //  此方法处理展示点击数
      val list2: List[Double] = RptUtils.click(requestmode, iseffective)

      //  此方法处理竞价操作
      val list3: List[Double] = RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)


      val list4: List[Double] = list1 ++ list2 ++ list3

      //  返回类型

      (networkname, list4)
    })


    val tup1: RDD[(String, Double, Double, Double, Double, Double, Double, Double, Double, Double)] = tup.groupByKey
      .map(x => (x._1, x._2.reduce((x, y) => x.zip(y).map(x => x._1 + x._2))))
      .map(x => (x._1, x._2(0), x._2(1), x._2(2),
        x._2(3), x._2(4), x._2(5), x._2(6), x._2(7), x._2(8)))

    val res: RDD[(String, Double, Double, Double, Double, Double, Double, Double, Double, Double)] =
      tup1.map(x => (x._1, x._2, x._3, x._4, x._5, x._8, x._9, x._6, x._7, x._10))

    val df: DataFrame = sQLcontext.createDataFrame(res).toDF("networkmannername", "org_num", "val_num", "ad_num",
      "bid_num", "bidwin_num", "show_num", "click_num", "ad_consume", "ad_cost")

    val load = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user", load.getString("jdbc.user"))
    prop.setProperty("password", load.getString("jdbc.password"))
    df.write.mode(SaveMode.Overwrite).jdbc(load.getString("jdbc.url"), "NetworkRpt", prop)

    sc.stop()
  }
}
