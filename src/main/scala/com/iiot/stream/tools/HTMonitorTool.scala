package com.iiot.stream.tools

import java.util.Properties

import com.iiot.stream.bean.{DPList, DPListWithDN, DPUnion, MetricWithDN}
import org.apache.spark.SparkConf

object HTMonitorTool {
  def initSparkConf(configs: Properties): SparkConf = {
    new SparkConf().setAppName("RealTimeCountAndMonitor").setMaster("local[4]")
      .set("spark.cores.max", configs.getProperty("spark.cores.max"))
      .set("spark.executor.cores", configs.getProperty("spark.executor.cores"))
      .set("spark.network.timeout", configs.getProperty("spark.network.timeout"))
      .set("spark.executor.memory", configs.getProperty("spark.executor.memory"))
      .set("spark.defalut.parallelism", configs.getProperty("spark.defalut.parallelism"))
      .set("spark.streaming.blockInterval", configs.getProperty("spark.streaming.blockInterval"))
      .set("spark.serializer", configs.getProperty("spark.serializer"))
        .set("spark.locality.wait.process",configs.getProperty("spark.locality.wait.process"))
      .set("spark.locality.wait.node",configs.getProperty("spark.locality.wait.node"))
//      .set("spark.locality.wait",configs.getProperty("spark.locality.wait"))
      .set("spark.streaming.kafka.maxRatePerPartition", configs.getProperty("spark.streaming.kafka.maxRatePerPartition"))
      .set("spark.kryo.registrationRequired", "true")
//      .set("spark.executor.extraJavaOptions","-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintHeapAtGC -XX:+PrintGCTimeStamps") //打印GC信息
      .set("spark.streaming.stopGracefullyOnShutdown","true") //当执行kill命令优雅的关闭job
      //      .set("spark.shuffle.service.enabled","true")
      //      .set("spa
      // rk.dynamicAllocation.enabled","true")//动态分配executor
      .registerKryoClasses(Array(classOf[DPList], classOf[DPListWithDN],
      classOf[DPUnion],classOf[MetricWithDN],classOf[com.htiiot.resources.utils.DeviceNumber],classOf[Properties]))
  }
  def initKafkaParamters(configs: Properties):Map[String,String]={
    Map("metadata.broker.list" -> configs.getProperty("kafka.broker.list"),
    "group.id" -> configs.getProperty("kafka.group.id"),
//    "zookeeper.connect" -> configs.getProperty("zookeeper.list"),
    "auto.offset.reset" -> configs.getProperty("auto.offset.reset"),
    "queued.max.message.chunks" -> configs.getProperty("queued.max.message.chunks"),
    "fetch.message.max.bytes" -> configs.getProperty("fetch.message.max.bytes"),
    "num.consumer.fetchers" -> configs.getProperty("num.consumer.fetchers"),
    "socket.receive.buffer.bytes" -> configs.getProperty("socket.receive.buffer.bytes")
    )
  }
}
