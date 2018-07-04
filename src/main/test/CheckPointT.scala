package org.apache.spark.streaming.rdd
import java.util.Properties

import com.iiot.stream.bean.{DPList, DPListWithDN, DPUnion, MetricWithDN}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.rdd.{HTStateRddAquire, MapWithStateRDDRecord}
import org.apache.spark.streaming.util.OpenHashMapBasedStateMap

object CheckPointT {
  def main(args: Array[String]): Unit = {
    val t = new HTStateRddAquire
    /*val p = t.getRddPath("hdfs://slave1.htdata.com:8020/chen/checkpoint")
    println(p)
    val rddP = t.getSpecificRddPath(p)
    println(rddP)*/
    val sconf = new SparkConf().setMaster("local[4]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setAppName("socket").registerKryoClasses(Array(classOf[DPList], classOf[DPListWithDN],classOf[Array[DPUnion]],
      classOf[DPUnion],classOf[MetricWithDN],classOf[com.htiiot.resources.utils.DeviceNumber],classOf[Properties],classOf[scala.collection.mutable.WrappedArray.ofRef[_]],classOf[MapWithStateRDDRecord[_,_,_]],
      classOf[OpenHashMapBasedStateMap[_,_]]))
    val sc = new SparkContext(sconf)
    val rdd = t.get[Long,(String,Long,Int),(Long,String,Long,Boolean)]("hdfs://slave1.htdata.com:8020/chen/checkpoint",sc).foreach(x=>{})
  }
}
