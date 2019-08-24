package com.utils

import org.apache.http.client.methods.{CloseableHttpResponse, HttpGet}
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils

/**
  * http请求
  */
object HttpUtil {


  // Get请求
  def get (url: String): String = {
    val client: CloseableHttpClient = HttpClients.createDefault()

    val get = new HttpGet(url)

    //  发送请求
    val response: CloseableHttpResponse = client.execute(get)

    //  获取返回结果

    EntityUtils.toString(response.getEntity, "UTF-8")
  }
}
