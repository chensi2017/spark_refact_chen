package com.iiot.stream.spark

import java.util.{Date, Properties}

import com.iiot.stream.base.RedisOperation
import com.iiot.stream.tools.DateUtils
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, State, StateSpec}
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.{Jedis, Pipeline}

import scala.collection.mutable
import scala.collection.mutable.HashMap

/**
  * @author chensi
  * @param redisProBro
  */
class HTUniqueDpCal(redisProBro: Broadcast[Properties]) extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(classOf[HTUniqueDpCal])

  def mapfun = (metricId: Long, value: Option[(Long, String, Int)], state: State[(Int, Int)]) => {
    val newData = value.get
    val date = DateUtils.formateTimeStamp(newData._1)
    if (state.exists()) {
      val stateData = state.get
      if(date==stateData._2){
        val times = stateData._1 + newData._3
        state.update(times,date)
        (metricId,newData._2,times,false,date)
      }else if(date>stateData._2){
        state.update(1,date)
        (metricId,newData._2,1,false,date)
      }else{
        (metricId,newData._2,-1,false,date)
      }
    }else{//测点第一次出现
      state.update(1,date)
      (metricId,newData._2,1,true,date)
    }
  }

  def uniqueDpCal(metricStream: DStream[(Long, (Long, String, Int))],initRDD:RDD[(Long,(Int,Int))]) = {
    val resultStream = metricStream.mapWithState(StateSpec.function(mapfun).initialState(initRDD))
    resultStream.foreachRDD(rdd => {
//      rdd.sparkContext.setLocalProperty("spark.scheduler.pool", "pool_b")
      rdd.foreachPartition(iter => {
        //1,所有独立测点数
        var uniqueMetricAll = 0
        //2,按日期统计独立的测点数
        val uniqueMetricByDate = new HashMap[Int,Int]
        //3,按租户和日期统计独立测点数
        val uniqueMetricByTidDate: mutable.HashMap[Tuple2[Int,String],Int] = new mutable.HashMap
        //4,统计所有datapoint总数
        var numTotalDP = 0
        //5,按租户统计datapoint总数
        var hashMapByTid= new HashMap[String,Int]
        //6,按date统计x总数
        var hashMapByDate= new HashMap[String,Int]
        //7,按date和tid统计x总数
        var hashMapByTidDate = new HashMap[Tuple2[String,String],Int]

        iter.foreach(it => {
          if (it._4) {
            uniqueMetricAll += 1
          }
          if (it._3 == 1) {
            uniqueMetricByDate.put(it._5,uniqueMetricByDate.getOrElse(it._5,0)+1)
            uniqueMetricByTidDate.put((it._5,it._2),uniqueMetricByTidDate.getOrElse((it._5,it._2),0)+1)
          }
          numTotalDP = numTotalDP + 1
          hashMapByTid.put(it._2, hashMapByTid.getOrElse(it._2, 0) + 1)
          val dateString = DateUtils.formateInt2String(it._5)
          hashMapByDate.put(dateString,hashMapByDate.getOrElse(dateString,0)+1)
          hashMapByTidDate.put((dateString,it._2),hashMapByTidDate.getOrElse((dateString,it._2),0)+1)
        })
        //println(s"独立测点数:${uniqueMetricAll}|||||日期独立测点数:${uniqueMetricToday}||||||${tidDate}")
        //repository to redis
        var redisHandle: Jedis = null
        try {
          redisHandle = RedisOperation.getInstance(redisProBro.value).getResource()
        } catch {
          case e: Exception => {
            logger.error(e.getMessage)
            logger.error("正在重新获取jedis...")
            try {
              redisHandle = RedisOperation.getInstance(redisProBro.value).getResource()
            } catch {
              case e: Exception => logger.error("重新获取jedis失败!!!")
            }
          }
        }
        var pl: Pipeline = null
        try {
          pl = redisHandle.pipelined()
          pl.incrBy("htstream:unique:dp:total", uniqueMetricAll)
          uniqueMetricByDate.foreach(x=>{
            pl.hincrBy("htstream:unique:dp:by:date",x._1.toString,x._2)
          })
          uniqueMetricByTidDate.foreach(x=>{
            pl.hincrBy("htstream:unique:dp:by:tenantid:date",x._1._1+":"+x._1._2,x._2)
          })
          pl.incrBy("htstream:total:dp",numTotalDP)
          hashMapByTid.foreach(x=>{
            pl.incrBy("htstream:total:dp:tenanid:" + x._1, x._2)
          })
          hashMapByDate.foreach(x=>{
            pl.incrBy("htstream:total:dp:date:" + x._1, x._2)
          })
          hashMapByTidDate.foreach(x=>{
            pl.incrBy("htstream:total:dp:tenantid:"+x._1._2+":date:"+x._1._1,x._2)
          })
          pl.sync()
        } catch {
          case e: Exception => logger.error(e,e)
        } finally {
          //release redis resource
          if (pl != null) {
            pl.close()
          }
          if (redisHandle != null) {
            redisHandle.close()
          }
        }
      })
    })
  }
}
