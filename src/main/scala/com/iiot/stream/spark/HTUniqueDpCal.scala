package com.iiot.stream.spark

import java.util.{Date, Properties}

import com.iiot.stream.base.RedisOperation
import com.iiot.stream.tools.DateUtils
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.{Seconds, State, StateSpec}
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.{Jedis, Pipeline}

import scala.collection.mutable

class HTUniqueDpCal(redisProBro:Broadcast[Properties]) extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(classOf[HTUniqueDpCal])

  def mapfun = (metricId:Long,value:Option[(Long,String,Int)],state:State[(String,Long,Int)])=>{
    val newData = value.get
    if(state.exists()){
      val stateData = state.get
      if(DateUtils.isToday(newData._1)) {//判断时间戳是否为今天
      var times = 0l
        if(DateUtils.formateTimeStamp(newData._1)==stateData._3){//状态中的时间也是今天
        val result = state.get()._2+1
          state.update(newData._2,result,stateData._3)
          times = result
        }else{//状态中的时间不是今天=>相当于这条数据是今天第一次出现
          state.update(newData._2,1l,DateUtils.formateTimeStamp(new Date().getTime))
        }
        (metricId,newData._2,times,false)
      }else {//今天之前的数据
        (metricId,newData._2,0,false)
      }
    }else{//第一次出现
      if(DateUtils.isToday(newData._1)){//is Today
        state.update(newData._2,1l,DateUtils.formateTimeStamp(newData._1))
        (metricId,newData._2,1,true)
      }else{//not Today
        state.update(newData._2,0,DateUtils.formateTimeStamp(newData._1))
        (metricId,newData._2,0,true)
      }
    }
  }

  def uniqueDpCal(metricStream:DStream[(Long,(Long,String,Int))]) = {
    val resultStream = metricStream.mapWithState(StateSpec.function(mapfun)).checkpoint(Seconds(5)).checkpoint(Seconds(1000))
    resultStream.foreachRDD(rdd=>rdd.foreachPartition(iter=>{
      var uniqueMetricAll = 0l
      var uniqueMetricToday = 0l
      val tidDate:mutable.HashMap[String,Long] = new mutable.HashMap
      val today = DateUtils.formateTimeStamp(new Date().getTime)
      iter.foreach(it=>{
        if(it._4){
          uniqueMetricAll += 1
        }
        if(it._3==1){
          uniqueMetricToday += 1
          tidDate.put(it._2,tidDate.getOrElse(it._2,0l)+1l)
        }
      })
      //println(s"独立测点数:${uniqueMetricAll}|||||日期独立测点数:${uniqueMetricToday}||||||${tidDate}")
      //repository to redis
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
      var pl:Pipeline = null
      try {
        pl = redisHandle.pipelined()
        pl.incrBy("htstream:unique:dp:total", uniqueMetricAll)
        pl.hincrBy("htstream:unique:dp:by:date", today.toString, uniqueMetricToday)
        tidDate.foreach(item => {
          pl.hincrBy("htstream:unique:dp:by:tenantid:date", today + ":" + item._1, item._2)
        })
        pl.sync()
      }catch {
        case e:Exception=>logger.error(e)
      }finally {
        //release redis resource
        if(pl!=null){
          pl.close()
        }
        if(redisHandle!=null){
          redisHandle.close()
        }
      }
    }))
  }
}
