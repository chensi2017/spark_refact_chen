package com.iiot.stream.spark

import com.iiot.stream.bean.DPUnion
import com.iiot.stream.tools.{HTInputDStreamFormat, ZookeeperClient}
import org.apache.spark.KafkaManager
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.rdd.HTMonitorTool

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
    ssc.checkpoint("hdfs://192.168.0.78:8020/chen/checkpoint")

    //获取配置进行广播
    val redisConfigs = zkClient.getAll(zk, "/conf_htiiot/redis")
    val redisBro = ssc.sparkContext.broadcast(redisConfigs)
    val sparkBro = ssc.sparkContext.broadcast(configs)

    val km = new KafkaManager(HTMonitorTool.initKafkaParamters(configs))
    val stream = km.createDirectStream(ssc,Set(configs.getProperty("kafka.topic")))

    //转换流
    val jsonDStream:DStream[DPUnion] = HTInputDStreamFormat.inputDStreamFormatWithDN(stream)
    jsonDStream.cache()

    val metricStream = jsonDStream.map(x=>(x.getMetricId,(x.getTs,x.getTid,1)))
    metricStream.cache()
    metricStream.foreachRDD(rdd=>{
      rdd.sparkContext.setLocalProperty("spark.scheduler.pool", "pool_a")
      rdd.foreachPartition(iter=>{

      })
    }
    )

    metricStream.foreachRDD(rdd=>{
      rdd.sparkContext.setLocalProperty("spark.scheduler.pool", "pool_b")
      rdd.foreachPartition(iter=>{

      })
    }
    )


    /*//独立测点计算
    val dpCal = new HTUniqueDpCal(redisBro)
    dpCal.uniqueDpCal(metricStream)

    //窗口计算
    val dpWindowCal = new HTUniqueDpWindowCal(redisBro)
    dpWindowCal.uniqueDpWindowCalNew(metricStream)*/

    /*//统计
    val statistics=new HTStateStatisticsFewerReduceextends(redisBro)
    statistics.DPStatistics(jsonDStream)

    //监控
    val monitorOperation =new HTMonitorOperation(sparkBro)
    monitorOperation.monitor(jsonDStream)*/

    /*//提交offset
    stream.foreachRDD(rdd=>{
      println("正在提交offset...............")
      km.updateOffsets(rdd)
    })*/

    ssc.start()
    ssc.awaitTermination()

  }
}
