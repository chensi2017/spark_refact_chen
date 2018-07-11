package com.iiot.stream.spark

import com.iiot.stream.bean.DPUnion
import com.iiot.stream.tools.{HTInputDStreamFormat, ZookeeperClient}
import org.apache.spark.KafkaManager
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.rdd.{HTMonitorTool, HTStateRddAquireFromCheckPoint}

object HTMonitorContext {
  val zkClient = new ZookeeperClient

  /*def getPath(system: FileSystem, path: Path) ={

  }

  def objectFileKryo[T](path: String, sc: SparkConf)(implicit ct: ClassTag[T]) = {
    val kryoSerializer = new KryoSerializer(sc)
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val hdfs = FileSystem.get(hadoopConf)
    val paths = getPath(hdfs, new Path(path))
    val d = paths.flatMap { p =>
    {
      val r = hdfs.open(p)
      var by = ArrayBuffer[Byte]()
      while (r.available() > 0) {
        val b = r.readByte()
        by += (b)
      }
      val kryo = kryoSerializer.newKryo()
      val input = new Input()
      input.setBuffer(by.toArray)
      val array = ArrayBuffer[T]()
      while (input.available() > 0) {
        val data = kryo.readClassAndObject(input)
        val dataObject = data.asInstanceOf[T]
        array += dataObject
      }
      array
    }
    }
    d
  }*/
  def main(args: Array[String]): Unit = {
    if(args.length==0){
      println("ERROR:Please input zookeeper address and checkpoint address!!!")
      return
    }
    System.setProperty("HADOOP_USER_NAME", "hdfs")
    val zkAddr = args(0)
    val checkpointAddr = args(1)
    val zk = zkClient.getConfingFromZk(zkAddr, 30000)
    var configs = zkClient.getAll(zk, "/conf_htiiot/spark_streamming")
    val ssc = new StreamingContext(HTMonitorTool.initSparkConf(configs),Duration(Integer.parseInt(configs.getProperty("duration.num"))))

    //获取独立测点历史状态数据
    val initRDD = new HTStateRddAquireFromCheckPoint().get[Long,(Int,Int),(Long,String,Int,Boolean,Int)](checkpointAddr,ssc.sparkContext)
    initRDD.persist(StorageLevel.MEMORY_ONLY_SER)
    ssc.checkpoint(checkpointAddr)

    //获取配置进行广播
    val redisConfigs = zkClient.getAll(zk, "/conf_htiiot/redis")
    val redisBro = ssc.sparkContext.broadcast(redisConfigs)
    val sparkBro = ssc.sparkContext.broadcast(configs)

    val km = new KafkaManager(HTMonitorTool.initKafkaParamters(configs))
    val stream = km.createDirectStream(ssc,Set(configs.getProperty("kafka.topic")))

    //转换流
    val jsonDStream:DStream[DPUnion] = HTInputDStreamFormat.inputDStreamFormatWithDN(stream)
    val metricStream = jsonDStream.map(x=>(x._metricId,(x._ts,x._tid,1)))
    jsonDStream.cache()
    metricStream.cache()

    //测点计算
    val dpCal = new HTUniqueDpCal(redisBro)
    dpCal.uniqueDpCal(metricStream,initRDD)

    //窗口计算
    val dpWindowCal = new HTUniqueDpWindowCal(redisBro)
    dpWindowCal.uniqueDpWindowCalNew(metricStream)

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
