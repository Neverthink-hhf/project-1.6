package com.Tags

import com.typesafe.config.{Config, ConfigFactory}
import com.utils.{RedisUtils, TagUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 上下文标签
  */

object TagsContext {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
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
    val stopword = sc.textFile("E:\\= =\\项目\\Spark用户画像分析\\stopwords.txt").map((_, 0)).collectAsMap()
    val bcstopword = sc.broadcast(stopword)

    //  todo  调用HBase API
    //  加载配置文件
    val load: Config = ConfigFactory.load()
    val hbaseTableName: String = load.getString("hbase.TableName")

    //  创建Hadoop任务
    val configuration: Configuration = sc.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum", load.getString("hbase.host"))

    //  创建HBaseConnection
    val hbconn: Connection = ConnectionFactory.createConnection(configuration)
    val hbadmin: Admin = hbconn.getAdmin
    if(!hbadmin.tableExists(TableName.valueOf(hbaseTableName))){
      //  创建表操作
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))

      val descriptor: HColumnDescriptor = new HColumnDescriptor("tags")

      tableDescriptor.addFamily(descriptor)
      hbadmin.createTable(tableDescriptor)
      hbadmin.close()
      hbconn.close()
    }

    //  创建Jobconf
    val jobconf = new JobConf(configuration)

    //  指定输出类型和表
    jobconf.setOutputFormat(classOf[TableOutputFormat])
    jobconf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName)



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

        //  商圈
        val business = TagsBusiness.makeTags(row)

        (userId, adList ++ Applist
          ++ Equipment ++ KeyWordList ++ localList
          ++ business)
      })
      .reduceByKey((list1, list2) =>
        (list1 ::: list2)
          .groupBy(x => x._1)
          .mapValues(x => x.foldLeft[Int](0)((x, y) => x + y._2))
          .toList
      ).map {

      case (userid, userTag) => {

        val put = new Put(Bytes.toBytes(userid))
        // 处理下标签
        val tags = userTag.map(t => t._1 + "," + t._2).mkString(",")
        put.addImmutable(Bytes.toBytes("tags"), Bytes.toBytes("1566822489462"), Bytes.toBytes(tags))
        (new ImmutableBytesWritable(), put)
      }
    }
      .saveAsHadoopDataset(jobconf)
  }
}
