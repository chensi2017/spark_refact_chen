package org.apache.spark.streaming.rdd
import java.util.Properties

import com.iiot.stream.bean.{DPListWithDN, DPUnion, MetricWithDN}
import org.apache.spark.rdd.ReliableCheckpointRDD
import org.apache.spark.streaming.util.OpenHashMapBasedStateMap
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

object SocketStreamT {
  def main(args: Array[String]): Unit = {
    val sconf = new SparkConf().setMaster("local[4]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setAppName("socket").registerKryoClasses(Array( classOf[DPListWithDN],classOf[Array[DPUnion]],
      classOf[DPUnion],classOf[MetricWithDN],classOf[com.htiiot.resources.utils.DeviceNumber],classOf[Properties],classOf[scala.collection.mutable.WrappedArray.ofRef[_]],classOf[MapWithStateRDDRecord[_,_,_]],
      classOf[OpenHashMapBasedStateMap[_,_]]))
    val sc = new SparkContext(sconf)
    /*val path = "hdfs://slave1.htdata.com:8020/chen/checkpoint/1f7636e6-8419-40ca-88d8-3c7c4f9a4170/rdd-3755"
    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(URI.create(path),hadoopConf)
    val kryoSerializer = new KryoSerializer(sconf)
//    val data = Tools.getFileList(path,hdfs)
      List(new Path("hdfs://slave1.htdata.com:8020/chen/checkpoint/1f7636e6-8419-40ca-88d8-3c7c4f9a4170/rdd-3755/part-00001")).flatMap(p=>{
      println(p)
      val r = hdfs.open(p)
      var by = ArrayBuffer[Byte]()
      while (r.available() > 0) {
        val b = r.readByte()
        by += (b)
      }
      val kryo = kryoSerializer.newKryo()
      val input = new Input()
      input.setBuffer(by.toArray)
      val array = ArrayBuffer[(Long,(String,Long,Int))]()
      while (input.available() > 0) {
        val data = kryo.readClassAndObject(input)
        val dataObject = data.asInstanceOf[(Long,(String,Long,Int))]
        array += dataObject
      }
      array
    }).foreach(println)

    sc.stop()*/

    /*val r = new ReliableCheckpointRDD[MapWithStateRDDRecord[String,Int,(String,Boolean)]](sc,"file:///D:\\tmp\\f59b154c-4f79-420d-8d58-4265bba97661\\rdd-89")
      .mapPartitions(iter=>{
        val b = new ArrayBuffer[(String,Int)]
        iter.foreach(x=>{
          val iter = x.stateMap.getAll()
          if(!iter.isEmpty){
            while (iter.hasNext){
              val item = iter.next()
              b.+=((item._1,item._2))
            }
          }
        })
        b.iterator
      }).foreach(println)*/

    val r = new ReliableCheckpointRDD[MapWithStateRDDRecord[Long,(String,Long,Int),(Long,String,Long,Boolean)]](sc,"hdfs://slave1.htdata.com:8020/chen/checkpoint/f58d8b62-4bde-4a98-abae-e22af5b516c3/rdd-230")
      .mapPartitions(iter=>{
        val b = new ArrayBuffer[(Long,(String,Long,Int))]
        iter.foreach(x=>{
          val iter = x.stateMap.getAll()
          if(!iter.isEmpty){
            while (iter.hasNext){
              val item = iter.next()
              b.+=((item._1,item._2))
            }
          }
        })
        b.iterator
      }).foreach(x=>{println(x)})


    /*
            val ssc = new StreamingContext(sc,Seconds(5))
    //    val rdd = sc.textFile("D:\\tmp\\36e19e16-1bf6-40af-8129-ff948b242db9\\rdd-310")
    //    val strings = rdd.collect().foreach(println)
        ssc.checkpoint("d:/tmp")
        val stream = ssc.socketTextStream("127.0.0.1",9999)
    //      .window(Seconds(10))
        val tupleStream = stream.map(x=>(x,1))
        val result = tupleStream.mapWithState(StateSpec.function((word:String,value:Option[Int],state:State[Int])=>{
          if(state.exists()){
            (word,false)
          }else{
            state.update(1)
            (word,true)
          }
        }).initialState(r)).checkpoint(Seconds(5)).print()
        def reduceFun = (x:Int,y:Int) =>{
          x+y
        }

        def invFuc = (x:Int,y:Int) =>{
          x-y
        }

        def filter = (t:(String,Int)) =>{
          if(t._2==0){
            false
          }else{
            true
          }
        }

    //    tupleStream.reduceByKeyAndWindow(reduceFun,invFuc,Seconds(10),Seconds(5),3).print()

    //    tupleStream.print()
    //    tupleStream.mapWithState()
        ssc.start()
        ssc.awaitTermination()*/
  }
}
