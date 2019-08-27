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
import org.apache.spark.graphx.{Edge, Graph}
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

    //  过滤符合条件的ID
    val baseRDD: RDD[(List[String], Row)] = df.filter(TagUtils.OneUserId)
      .map(row => {
        val userList: List[String] = TagUtils.getAllUserId(row)
        (userList, row)
      })


    //  构建点集合
    val vertiesRDD: RDD[(Long, List[(String, Int)])] = baseRDD.flatMap(tp => {
      val row = tp._2

      //  所有标签
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
      val businesslist = TagsBusiness.makeTags(row)

      //  所有标签
      val AllTag: List[(String, Int)] = adList ++ Applist ++ KeyWordList ++ channellist ++ Equipment ++ localList ++ businesslist

      //  保证其中一个点携带着所有的标签，同时也保留所有的Uid
      val VD = tp._1.map(x => (x, 0)) ++ AllTag

      tp._1.map(uid => {

        //  保证一个点携带标签(uid, vd),(uid, list()),(uid, list())
        if (tp._1.head.equals(uid)) {
          (uid.hashCode.toLong, VD)
        } else {
          (uid.hashCode.toLong, List.empty)
        }
      })
    })
    vertiesRDD

    // 构建边的集合
    val edges: RDD[Edge[Int]] = baseRDD.flatMap(tp => {
      // A B C : A->B A->C
      tp._1.map(uId => Edge(tp._1.head.hashCode, uId.hashCode, 0))
    })
    //edges.take(20).foreach(println)

    // 构建图
    val graph = Graph(vertiesRDD,edges)
    // 取出顶点 使用的是图计算中的连通图算法
    val vertices = graph.connectedComponents().vertices

    // 处理所有的标签和id
    vertices.join(vertiesRDD).map{
      case (uId,(conId,tagsAll))=>(conId,tagsAll)
    }.reduceByKey((list1,list2)=>{
      // 聚合所有的标签
      (list1++list2).groupBy(_._1).mapValues(_.map(_._2).sum).toList
    }).map{
      case(userid,userTag)=>{

        val put = new Put(Bytes.toBytes(userid))
        // 处理下标签
        val tags = userTag.map(t=>t._1+","+t._2).mkString(",")
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes("1566911430796"),Bytes.toBytes(tags))
        (new ImmutableBytesWritable(),put)
      }
    }
      // 保存到对应表中
      .saveAsHadoopDataset(jobconf)






  }
}
