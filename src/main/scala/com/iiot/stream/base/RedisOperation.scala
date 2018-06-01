package com.iiot.stream.base

import java.util.Properties
import java.{lang, util}

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.apache.log4j.Logger
import redis.clients.jedis.exceptions.JedisConnectionException
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster, JedisPool}

class RedisOperation(configs:Properties) {
  @transient lazy  val logger: Logger = Logger.getLogger(classOf[RedisOperation])
  var clusterNodeList: util.HashSet[HostAndPort] = _
  val hStr: String = configs.getProperty("address")
  val hList: Array[String] = hStr.split(",")
  for (host <- hList) {
    clusterNodeList = new java.util.HashSet[HostAndPort]()
    clusterNodeList.add(new HostAndPort(host, configs.getProperty("port").toInt))
  }
  //create a pool or cluster
  var redisCluster: Any = null
  var redisPattern: String = if("".equals(configs.getProperty("mode"))) "redispool" else configs.getProperty("mode")

  init()
  def init() {
    var poolConfig: GenericObjectPoolConfig = new GenericObjectPoolConfig
    poolConfig.setMinIdle(100)
    poolConfig.setMaxTotal(200)
    poolConfig.setMaxWaitMillis(30000L)
    poolConfig.setTestOnReturn(true)
    poolConfig.setTestWhileIdle(true)
    poolConfig.setMinEvictableIdleTimeMillis(60000)
    poolConfig.setTimeBetweenEvictionRunsMillis(30000)
    poolConfig.setNumTestsPerEvictionRun(-1)
    if ("cluster".equals(redisPattern)) {
      redisCluster = new JedisCluster(clusterNodeList,
        configs.getProperty("connectionTimeout").toInt,
        configs.getProperty("soTimeout").toInt,
        configs.getProperty("maxAttempts").toInt,
        //configs.get("redis.password"),
        poolConfig)
    } else if ("redispool".equals(redisPattern)) {
      redisCluster = new JedisPool(poolConfig,
        configs.getProperty("address"),
        configs.getProperty("port").toInt,
        configs.getProperty("connectionTimeout").toInt,
        if("null".equals(configs.getProperty("auth")))null;else configs.getProperty("auth"))//若redis中无密码请在zk中设置节点内容为null
    }
    else {
      throw new Exception("you should check the ‘redis.running.pattern’to decide the redis' pattern.and " +
        "redispool and cluster is the option")
    }
  }

  def clusterIncrBy(key: String, addNum: Long): Long = {
    if(redisCluster == null){
      init()
    }
    val redisHandle = redisCluster.asInstanceOf[JedisCluster]
    val result = redisHandle.incrBy(key,addNum)
    result
  }

  def clusterSadd(key: String, vals: String): Long = {
    if(redisCluster == null){
      init()
    }
    val redisHandle = redisCluster.asInstanceOf[JedisCluster]
    val result = redisHandle.sadd(key,vals)
    result
  }

  def clusterSet(key: String, vals: String): String = {
    if(redisCluster == null){
      init()
    }
    val redisHandle = redisCluster.asInstanceOf[JedisCluster]
    val result = redisHandle.set(key,vals)
    result
  }

  def clusterGet(key: String): String = {
    if(redisCluster == null){
      init()
    }
    val redisHandle = redisCluster.asInstanceOf[JedisCluster]
    val result = redisHandle.get(key)
    result
  }

  def clusterDel(key: String): lang.Long = {
    if(redisCluster == null){
      init()
    }
    val redisHandle = redisCluster.asInstanceOf[JedisCluster]
    val result = redisHandle.del(key)
    result
  }


  def getResource()= {
    if(redisCluster == null){
      init()
    }
    var redisHandle:Jedis = null
    try {
      redisHandle = redisCluster.asInstanceOf[JedisPool].getResource
    }catch {
      case e:JedisConnectionException=>{
        println("[RedisOperation]从jedis连接池获取连接失败")
        logger.error(e)
      }
    }
//    val pool = redisCluster.asInstanceOf[JedisPool]
    redisHandle
  }
}
object RedisOperation{
  var redisOperation:RedisOperation = null
  def getInstance(zkAddr:Properties): RedisOperation ={
    synchronized{
      if(redisOperation==null){
        redisOperation = new RedisOperation(zkAddr)
      }
    }
    redisOperation
  }

}
