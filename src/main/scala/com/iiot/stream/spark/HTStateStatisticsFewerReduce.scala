package com.iiot.stream.spark


import java.io.IOException
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.iiot.stream.base._
import com.iiot.stream.bean.DPUnion
import com.iiot.stream.tools.TimeTransform
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

import scala.collection.mutable.HashMap

class HTStateStatisticsFewerReduceextends(redisProBro:Broadcast[Properties]) extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(classOf[HTStateStatisticsFewerReduceextends])
  val activeInterval: Int = 30
  var calT = 0l
  var toRedisT = 0l

  val foreachPartitionFunc = (it: Iterator[DPUnion]) => {
    var redisHandle :Jedis = null
    try {
      redisHandle = RedisOperation.getInstance(redisProBro.value).getResource()
    }catch {
      case e:Exception =>{
        logger.error(e.getMessage)
        logger.error("正在重新获取jedis...")
        try {
          redisHandle = RedisOperation.getInstance(redisProBro.value).getResource()
        }catch {
          case e:Exception=>logger.error("重新获取jedis失败!!!")
        }
      }
    }
    //1,统计所有datapoint总数
    var numTotalDP = 0
    //2,按租户统计datapoint总数
    var hashMapByTid= new HashMap[String,Int]
    //3,按date统计x总数
    var hashMapByDate= new HashMap[String,Int]
    //4,按date和tid统计x总数
    var hashMapByTidDate = new HashMap[Tuple2[String,String],Int]

    try {
      val startCal = System.currentTimeMillis()
      while (it.hasNext) {
        val x = it.next()
        val date = TimeTransform.timestampToDate(x.getTs)
        //1,统计所有DP总数
        numTotalDP = numTotalDP + 1

        //2，按租户统计DP
        val tid = hashMapByTid.getOrElse(x.getTid, 0)
        hashMapByTid.put(x.getTid, tid.toInt + 1)

        //3，按日期统计DP
        val dateNum = hashMapByDate.getOrElse(date, 0)
        hashMapByDate.put(date, dateNum.toInt + 1)

        //4，x num by tid and date
        val tidDateNum = hashMapByTidDate.getOrElse((x.getTid, date), 0)
        hashMapByTidDate.put((x.getTid, date), tidDateNum.toInt + 1)
      }
      calT = System.currentTimeMillis() - startCal
    }catch {
      case e:NumberFormatException =>{
        logger.error("NumberFormat Error" + e.getMessage)
      }
      case e:NullPointerException =>{
        logger.error("NullPoint Error" + e.getMessage)
      }
    }
    //更新redis可以使用pl优化
    try {
      val startToRedisT = System.currentTimeMillis()
      //1,total dp
      println("the total  DP num is:" + numTotalDP)
      redisHandle.incrBy("htstream:total:dp", numTotalDP)

      //2,total dp by tid
      hashMapByTid.keys.foreach(x => {
        val num = hashMapByTid.get(x) match {
          case Some(a) => a
          case None => 0
        }
//        logger.info("htstream:total:dp:tenanid:" + x + ";num:" + num)
        redisHandle.incrBy("htstream:total:dp:tenanid:" + x, num)
      })

      //3,total dp by date
      hashMapByDate.keys.foreach(x => {
        val num = hashMapByDate.get(x) match {
          case Some(a) => a
          case None => 0
        }
//        logger.info("htstream:total:dp:date:" + x + ";num:" + num)
        redisHandle.incrBy("htstream:total:dp:date:" + x, num)
      })

      //4,dp num by tid and date
      hashMapByTidDate.keys.foreach(x => {
        val num = hashMapByTidDate.get(x) match {
          case Some(a) => a
          case None => 0
        }
        redisHandle.incrBy("htstream:total:dp:tenantid:" + x._1 + ":date:" + x._2, num.toLong)
      })
      toRedisT = System.currentTimeMillis() - startToRedisT
    }catch {
      case e:IOException =>{
        logger.error("IO Error" + e.getMessage)
      }
      case e:NullPointerException =>{
        logger.error("NullPoint Error" + e.getMessage)
      }
      case e:Exception =>{
        logger.error("Unknown Error" + e.getMessage)
      }
    }
    finally {
      try {
        if(redisHandle!=null) {
          redisHandle.close()
        }
      } catch {
        case e: IOException => {
          logger.error("IO Error" + e.getMessage)
        }
      }
    }
    println(s"${new SimpleDateFormat("yy-MM-dd hh:mm:ss").format(new Date())}${Thread.currentThread()}>>>测点统计总时长: ${calT} ms;统计结果存储总时长: ${toRedisT} ms;")
  }

  def DPStatistics(jsonDStream: DStream[DPUnion]): Unit = {
    jsonDStream.foreachRDD(rdd => {
//      rdd.sparkContext.setLocalProperty("spark.scheduler.pool","pool_a")
      rdd.foreachPartition(foreachPartitionFunc)
  })
  }


  def distinctDnThingID(jsonDStream: DStream[DPUnion]): Unit = {
    //5, distinct dn
      /*jsonDStream.map(x => (x.getDnStr)).foreachRDD( rdd => {
        var redisHandle: Jedis = null
        redisHandle = RedisOperation.getResource()
        val distinctDnNum = rdd.distinct().count()
        println("the distinct dn num is:" + distinctDnNum)
        redisHandle.set("htstream:unique:dn", distinctDnNum.toString)
        redisHandle.close()
      })*/

    //2018.4.9
    jsonDStream.foreachRDD( rdd => {
      val distinctDnNum = rdd.map(x=>(x.getDnStr,1)).reduceByKey((x,y)=>1).map(_._1).count()
      var redisHandle: Jedis = null
      redisHandle = RedisOperation.getInstance(redisProBro.value).getResource()
      println("the distinct dn num is:" + distinctDnNum)
      redisHandle.set("htstream:unique:dn", distinctDnNum.toString)
      redisHandle.close()
    })


    //6, distinct thingID
    jsonDStream.map(x => (x.getThingId)).foreachRDD( rdd => {
      var redisHandle: Jedis = null
      redisHandle = RedisOperation.getInstance(redisProBro.value).getResource()
      val distinctThingIdNum = rdd.distinct().count()
      println("the distinct dn num is:" + distinctThingIdNum)
      redisHandle.set("htstream:unique:dn", distinctThingIdNum.toString)
      redisHandle.close()
    })
  }


}
