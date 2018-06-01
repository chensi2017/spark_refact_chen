package com.iiot.stream.base

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.log4j.Logger
import redis.clients.jedis.JedisPool

/**
  * 此类用于读取本地redis获取预警阈值信息
  */

/*object RedisRead{
  var redisRead:RedisRead = null
  def getInstance(sparkPro:Properties) ={
    if(redisRead==null){
      synchronized{
        if(redisRead==null){
          redisRead = new RedisRead(sparkPro.getProperty("redis.local.port").toInt,sparkPro.getProperty("redis.local.auth"))
        }
      }
    }else{
      redisRead
    }
  }

}*/

object RedisRead {
  @transient lazy  val logger: Logger = Logger.getLogger(RedisRead.getClass)

  var redisCluster: Any = null

  def init(port:Int,auth:String) {
    val startTime = System.currentTimeMillis();
    var poolConfig: GenericObjectPoolConfig = new GenericObjectPoolConfig
    poolConfig.setMinIdle(100)
    poolConfig.setMaxTotal(1000)
    poolConfig.setMaxWaitMillis(3000L)
    poolConfig.setMinEvictableIdleTimeMillis(1)
    poolConfig.setTestOnReturn(true)
    poolConfig.setTestWhileIdle(true)
    poolConfig.setMinEvictableIdleTimeMillis(60000)
    poolConfig.setTimeBetweenEvictionRunsMillis(30000)
    poolConfig.setNumTestsPerEvictionRun(-1)
    redisCluster = new JedisPool(poolConfig, "127.0.0.1", port, 100000, if("null".equals(auth))null;else auth)
    val endTime = System.currentTimeMillis()
    println(s"[com.iiot.stream.base.RedisRead] init RedisPool time: ${endTime-startTime} ms;")
  }

  def getResource(port:Int,auth:String) = {
    if(redisCluster == null){
      synchronized{
        if(redisCluster ==null ) {
          if(auth==null)
            init(port,"null")
          else
            init(port,auth)
        }
      }
    }
    val redisHandle = redisCluster.asInstanceOf[JedisPool].getResource
    redisHandle
  }
}
