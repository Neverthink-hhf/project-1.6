package com.utils

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 标签工具类
  */
object TagUtils {

    //  过滤需要的字段
    val OneUserId=
      """
        | imei !='' or mac !='' or openudid !='' or androidid !='' or idfa !='' or
        | imeimd5 !='' or macmd5 !='' or openudidmd5 !='' or androididmd5 !='' or idfamd5 !='' or
        | imeisha1 !='' or macsha1 !='' or openudidsha1 !='' or androididsha1 !='' or idfasha1 !=''
      """.stripMargin

    //  去除唯一不为空的Id
    def getOneUserId(row: Row): String ={
      row match {
        case v if StringUtils.isNotBlank(v.getAs[String]("imei")) => "IM: "+v.getAs[String]("imei")
        case v if StringUtils.isNotBlank(v.getAs[String]("mac")) => "MAC: "+v.getAs[String]("mac")
        case v if StringUtils.isNotBlank(v.getAs[String]("openudid")) => "OP: "+v.getAs[String]("openudid")
        case v if StringUtils.isNotBlank(v.getAs[String]("androidid")) => "Android: "+v.getAs[String]("androidid")
        case v if StringUtils.isNotBlank(v.getAs[String]("idfa")) => "IDFA: "+v.getAs[String]("idfa")

        case v if StringUtils.isNotBlank(v.getAs[String]("imeimd5")) => "IM5: "+v.getAs[String]("imeimd5")
        case v if StringUtils.isNotBlank(v.getAs[String]("macmd5")) => "MAC5: "+v.getAs[String]("macmd5")
        case v if StringUtils.isNotBlank(v.getAs[String]("openudidmd5")) => "OP5: "+v.getAs[String]("openudidmd5")
        case v if StringUtils.isNotBlank(v.getAs[String]("androididmd5")) => "Android5: "+v.getAs[String]("androididmd5")
        case v if StringUtils.isNotBlank(v.getAs[String]("idfamd5")) => "IDFA5: "+v.getAs[String]("idfamd5")

        case v if StringUtils.isNotBlank(v.getAs[String]("imeisha1")) => "IMsha1: "+v.getAs[String]("imeisha1")
        case v if StringUtils.isNotBlank(v.getAs[String]("macsha1")) => "MACsha1: "+v.getAs[String]("macsha1")
        case v if StringUtils.isNotBlank(v.getAs[String]("openudidsha1")) => "OPsha1: "+v.getAs[String]("openudidsha1")
        case v if StringUtils.isNotBlank(v.getAs[String]("androididsha1")) => "Androidsha1: "+v.getAs[String]("androididsha1")
        case v if StringUtils.isNotBlank(v.getAs[String]("idfasha1")) => "IDFAsha1: "+v.getAs[String]("idfasha1")
      }
    }

  // 获取所有Id
  def getAllUserId(row:Row):List[String]={
    var list = List[String]()
    if (StringUtils.isNotBlank(row.getAs[String]("imei"))) list:+= "IM: "+row.getAs[String]("imei")
    if (StringUtils.isNotBlank(row.getAs[String]("mac"))) list:+= "MC: "+row.getAs[String]("mac")
    if (StringUtils.isNotBlank(row.getAs[String]("openudid"))) list:+= "OD: "+row.getAs[String]("openudid")
    if (StringUtils.isNotBlank(row.getAs[String]("androidid"))) list:+= "AD: "+row.getAs[String]("androidid")
    if (StringUtils.isNotBlank(row.getAs[String]("idfa"))) list:+= "ID: "+row.getAs[String]("idfa")
    if (StringUtils.isNotBlank(row.getAs[String]("imeimd5"))) list:+= "IMM: "+row.getAs[String]("imeimd5")
    if (StringUtils.isNotBlank(row.getAs[String]("macmd5"))) list:+= "MCM: "+row.getAs[String]("macmd5")
    if (StringUtils.isNotBlank(row.getAs[String]("openudidmd5"))) list:+= "ODM: "+row.getAs[String]("openudidmd5")
    if (StringUtils.isNotBlank(row.getAs[String]("androididmd5"))) list:+= "ADM: "+row.getAs[String]("androididmd5")
    if (StringUtils.isNotBlank(row.getAs[String]("idfamd5"))) list:+= "IDM: "+row.getAs[String]("idfamd5")
    if (StringUtils.isNotBlank(row.getAs[String]("imeisha1"))) list:+= "IMS: "+row.getAs[String]("imeisha1")
    if (StringUtils.isNotBlank(row.getAs[String]("macsha1"))) list:+= "MCS: "+row.getAs[String]("macsha1")
    if (StringUtils.isNotBlank(row.getAs[String]("openudidsha1"))) list:+= "ODS: "+row.getAs[String]("openudidsha1")
    if (StringUtils.isNotBlank(row.getAs[String]("androididsha1"))) list:+= "ADS: "+row.getAs[String]("androididsha1")
    if (StringUtils.isNotBlank(row.getAs[String]("idfasha1"))) list:+= "IDS: "+row.getAs[String]("idfasha1")
    list
  }
}
