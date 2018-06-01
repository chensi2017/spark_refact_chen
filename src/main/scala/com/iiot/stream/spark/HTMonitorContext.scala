package com.iiot.stream.spark

import com.iiot.stream.bean.DPUnion
import com.iiot.stream.tools.{HTInputDStreamFormat, HTMonitorTool, ZookeeperClient}
import org.apache.spark.KafkaManager
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, StreamingContext}

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

    //获取配置进行广播
    val redisConfigs = zkClient.getAll(zk, "/conf_htiiot/redis")
    val redisBro = ssc.sparkContext.broadcast(redisConfigs)
    val sparkBro = ssc.sparkContext.broadcast(configs)

    val km = new KafkaManager(HTMonitorTool.initKafkaParamters(configs))
    val stream = km.createDirectStream(ssc,Set(configs.getProperty("kafka.topic").toString))

    //转换流
    val jsonDStream:DStream[DPUnion] = HTInputDStreamFormat.inputDStreamFormatWithDN(stream)
//    jsonDStream.cache()

    //统计
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
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
