package com.iiot.stream.spark

import java.util.Date

import com.iiot.stream.bean.DPUnion
import com.iiot.stream.tools.{DateUtils, HTInputDStreamFormat, HTMonitorTool, ZookeeperClient}
import org.apache.spark.KafkaManager
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming._

import scala.collection.mutable

object HTMonitorContext {
  val zkClient = new ZookeeperClient
  def main(args: Array[String]): Unit = {
    if(args.length==0){
      println("ERROR:Please input zookeeper address!!!")
      return
    }

    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val zkAddr = args(0)
    val zk = zkClient.getConfingFromZk(zkAddr, 30000)
    var configs = zkClient.getAll(zk, "/conf_htiiot/spark_streamming")
    val ssc = new StreamingContext(HTMonitorTool.initSparkConf(configs),Duration(Integer.parseInt(configs.getProperty("duration.num"))))
    ssc.checkpoint("d:/tmpt")
    //获取配置进行广播
    val redisConfigs = zkClient.getAll(zk, "/conf_htiiot/redis")
    val redisBro = ssc.sparkContext.broadcast(redisConfigs)
    val sparkBro = ssc.sparkContext.broadcast(configs)

    val km = new KafkaManager(HTMonitorTool.initKafkaParamters(configs))
    val stream = km.createDirectStream(ssc,Set(configs.getProperty("kafka.topic")))

    def mapfun = (metricId:Long,value:Option[(Long,String)],state:State[(String,Long,Int)])=>{
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

    //转换流
    val jsonDStream:DStream[DPUnion] = HTInputDStreamFormat.inputDStreamFormatWithDN(stream)


    val metricStream = jsonDStream.map(x=>(x.getMetricId,(x.getTs,x.getTid)))
    val resultStream = metricStream.mapWithState(StateSpec.function(mapfun))

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
      //持久化
//      println(s"独立测点数:${uniqueMetricAll}|||||日期独立测点数:${uniqueMetricToday}||||||${tidDate}")
    }))

    //窗口计算
    metricStream.reduceByKeyAndWindow(Seconds(30)).foreachRDD(rdd=>{
      //clear redis history


      rdd.foreachPartition(iter=>{
        var uniqueMetric = 0l
        var uniqueMetricInTid = new mutable.HashMap[String,Long]()
        iter.foreach(it=>{
          uniqueMetric += 1
//          uniqueMetricInTid.put(it._2)
        })
      })


    })

    /*val windowMapStream = windowStream.mapWithState(StateSpec.function((metricId:Long,value:Option[(Long,String)],state:State[Int])=>{
      if(state.exists()){
        (metricId,value.get._2,false)
      }else{
        state.update(1)
        (metricId,value.get._2,true)
      }
    }).timeout(Seconds(1)))
    windowMapStream.foreachRDD(rdd=>rdd.foreachPartition(iter=>{

      //clear redis history data

      //calculate
      var uniqueMetric = 0l
      var uniqueMetricInTid = new mutable.HashMap[String,Long]()
      iter.foreach(it=>{
        if(it._3){
          uniqueMetric += 1
          uniqueMetricInTid.put(it._2,uniqueMetricInTid.getOrElse(it._2,0l)+1l)
        }
      })
      //save to redis
      println(s"30s独立测点数:${uniqueMetric}|||||${uniqueMetricInTid}")

    }))*/


    /*//统计
    val statistics=new HTStateStatisticsFewerReduceextends(redisBro)
    statistics.DPStatistics(jsonDStream)
    //statistics.distinctDnThingID(jsonDStream)

    //监控
    val monitorOperation =new HTMonitorOperation(sparkBro)
    monitorOperation.monitor(jsonDStream)

    //提交offset
    stream.foreachRDD(rdd=>{
      println("正在提交offset...............")
      km.updateOffsets(rdd)
    })*/

    ssc.start()
    ssc.awaitTermination()

  }
}
