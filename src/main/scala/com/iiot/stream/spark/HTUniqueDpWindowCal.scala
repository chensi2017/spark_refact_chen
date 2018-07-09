package com.iiot.stream.spark

import java.util.Properties

import com.iiot.stream.base.RedisOperation
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable

/**
  * @author chensi
  * @param redisProBro
  */
class HTUniqueDpWindowCal(redisProBro:Broadcast[Properties]) extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(classOf[HTUniqueDpWindowCal])

  /*def clearRedis(){
    var jedis = RedisOperation.getInstance(redisProBro.value).getResource()
    jedis.del("htstream:unique:dp:by:tenantid:window","htstream:unique:dp:total:by:window")
    try{
      if(jedis!=null){
        jedis.close()
      }
    }catch {
      case e:Exception=>logger.error(e)
    }
    finally
    {
      jedis = null
    }
  }*/

  def updateToRedis(uniqueMetric:Long,tidmetric:mutable.HashMap[String,Long]): Unit ={
    var j = RedisOperation.getInstance(redisProBro.value).getResource()
    var pl = j.pipelined()
    pl.del("htstream:unique:dp:by:tenantid:window","htstream:unique:dp:total:by:window")
    pl.set("htstream:unique:dp:total:by:window",uniqueMetric.toString)
    tidmetric.foreach(item=>{
      pl.hset("htstream:unique:dp:by:tenantid:window",item._1,item._2.toString)
    })
    pl.sync()
    try{
      if(pl!=null){pl.close()}
      if(j!=null){j.close()}
    }catch {
      case e:Exception=>logger.error(e)
    }finally {
      pl = null
      j = null
    }
  }

  def uniqueDpWindowCal(metricStream:DStream[(Long,(Long,String,Int))]) ={
    metricStream.window(Seconds(30),Seconds(5)).groupByKey().foreachRDD(rdd=>{
      //calculate data
      rdd.foreachPartition(iter=>{
        var uniqueMetric = 0l
        var tidmetric = new mutable.HashMap[String,Long]()
        iter.foreach(it=>{
          uniqueMetric += 1
          val iterable = it._2
          val first = iterable.iterator.next()
          tidmetric.put(first._2,tidmetric.getOrElse(first._2,0l)+1)
        })
        //del and update to redis
        updateToRedis(uniqueMetric,tidmetric)
      })
    })
  }

  //use reduceByKeyAndWindow
  def uniqueDpWindowCalNew(metricStream:DStream[(Long,(Long,String,Int))]) ={
    metricStream.reduceByKeyAndWindow(reduceFun,invFun,Seconds(30),Seconds(5),35,filter)
      .foreachRDD(rdd=>{
//        rdd.sparkContext.setLocalProperty("spark.scheduler.pool","pool_b")
        //calculate data
        rdd.foreachPartition(iter=>{
          var uniqueMetric = 0l
          var uniqueMetricInTid = new mutable.HashMap[String,Long]()
          iter.foreach(it=>{
            uniqueMetric += 1
            uniqueMetricInTid.put(it._2._2,uniqueMetricInTid.getOrElse(it._2._2,0l)+1)
          })
          //del and update to redis
          updateToRedis(uniqueMetric,uniqueMetricInTid)
        })
      })
  }

  def reduceFun = (x:(Long,String,Int),y:(Long,String,Int)) =>{
    (x._1,x._2,x._3+y._3)
  }

  def invFun = (x:(Long,String,Int),y:(Long,String,Int)) =>{
    (x._1,x._2,x._3-y._3)
  }

  def filter = (t:(Long,(Long,String,Int))) =>{
    if(t._2._3==0){
      false
    }else{
      true
    }
  }

}
