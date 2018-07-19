package com.iiot.stream.spark

import java.io.IOException
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.htiiot.resources.model.ThingBullet
import com.iiot.stream.base.{DPConsumerPlugin, RedisRead}
import com.iiot.stream.bean.DPUnion
import com.iiot.stream.tools.KafkaWriter
import org.apache.log4j.Logger
import redis.clients.jedis.{Jedis, Pipeline}


class MyThread(name:String, num:Int, arr:Array[DPUnion], sparkPro:Properties) extends Thread{
  @transient lazy val logger: Logger = Logger.getLogger(classOf[MyThread])

  var _name = name
  var _num = num

override def run(){
    val start = System.currentTimeMillis()
    var DPAlarmNum = 0
    var getThresholdTime:Long = 0l
    var packThresholdTime:Long = 0l
    var judgeTime:Long = 0l
    var packMetricId = 0l
    if (arr.length > 0) {
      var redisHandle :Jedis = null
      var pl:Pipeline = null
      val plugin = DPConsumerPlugin.getPlugin()
      try {
        redisHandle = RedisRead.getResource(sparkPro.getProperty("redis.local.port").toInt,sparkPro.getProperty("redis.local.auth"))
        pl = redisHandle.pipelined()
        //封装metricId
        val startPackMetricId = System.currentTimeMillis()
        arr.foreach(point=>{
          pl.get("event_"+point._metricId+"_"+point._key)
        })
        packMetricId = System.currentTimeMillis() - startPackMetricId
        //pipeline批量获取阈值
        val startGetThresholdT = System.currentTimeMillis()
        val list = pl.syncAndReturnAll
        getThresholdTime = System.currentTimeMillis() - startGetThresholdT
        //封装阈值
        val startPackThresholdT = System.currentTimeMillis()
        var i = 0
        arr.foreach(point=>{
          val r = if(list.get(i)==null)null else list.get(i).toString
          point.threshold = r
          i+=1
        })
        packThresholdTime = System.currentTimeMillis() - startPackThresholdT
        //测点判断是否需要预警，需要预警的写入kafka
        val startJudgeT = System.currentTimeMillis()
        for (data <- arr) {
          if (data != null) {
            var thingBullets: Array[ThingBullet] = null
            thingBullets = plugin.checkDataPointToThingBullet(data)
            if (null == thingBullets) {
            } else {
              KafkaWriter.getResource(sparkPro.getProperty("monitor.kafka.broker.list"),sparkPro.getProperty("monitor.kafka.topic"))
              KafkaWriter.writer(thingBullets)
              DPAlarmNum += 1
              thingBullets = null
            }
          }
        }
        judgeTime = System.currentTimeMillis() - startJudgeT
      }catch {
        case e:Exception=>logger.error(e,e)
      }
      finally {
        try {
          if(pl!=null) pl.close()
          if(redisHandle!=null) redisHandle.close()
        } catch {
          case e: IOException => {
            logger.error(s"IO Error ${e.getMessage}",e)
          }
        }
      }
    }
    println(s"${new SimpleDateFormat("yy-MM-dd hh:mm:ss").format(new Date())}${Thread.currentThread()}>>>线程总执行时间: ${System.currentTimeMillis() - start} ms;{ 遍历获取阈值key的时间: ${packMetricId} ms;从redis获取阈值时间: ${getThresholdTime} ms;封装阈值时间: ${packThresholdTime}ms;判断预警时间+写kafka时间: ${judgeTime} ms; } 写入Kafka条数: ${DPAlarmNum} 条;处理测点总数为: ${arr.length} 个;")
  }
}
