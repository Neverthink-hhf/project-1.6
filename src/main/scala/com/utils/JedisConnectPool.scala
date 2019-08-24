package com.utils

import redis.clients.jedis.{Jedis, JedisPoolConfig}

object JedisConnectPool {
  val config = new JedisPoolConfig

  //  设置最大连接数
  config.setMaxTotal(20)

  //  最大空闲
  config.setMaxIdle(10)

  //  创建链接
  val pool = new Jedis()

  def getConnection() = {
    pool
  }

}
