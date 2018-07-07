package org.apache.spark.streaming.rdd
import java.util.Properties

import com.iiot.stream.bean.{DPList, DPListWithDN, DPUnion, MetricWithDN}
import com.iiot.stream.rdd.EmptyRDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.rdd.{HTStateRddAquireFromCheckPoint, MapWithStateRDDRecord}
import org.apache.spark.streaming.util.OpenHashMapBasedStateMap
import org.junit.{Assert, Test}
import org.scalatest.FunSuite
@Test
class CheckPointT extends Assert{
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","hdfs")
    val t = new HTStateRddAquireFromCheckPoint
    /*val p = t.getRddPath("hdfs://slave1.htdata.com:8020/chen/checkpoint")
    println(p)
    val rddP = t.getSpecificRddPath(p)
    println(rddP)*/
    val sconf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setAppName("socket").registerKryoClasses(Array(classOf[DPList], classOf[DPListWithDN],classOf[Array[DPUnion]],
      classOf[DPUnion],classOf[MetricWithDN],classOf[com.htiiot.resources.utils.DeviceNumber],classOf[Properties],classOf[scala.collection.mutable.WrappedArray.ofRef[_]],classOf[MapWithStateRDDRecord[_,_,_]],
      classOf[OpenHashMapBasedStateMap[_,_]]))
    val sc = new SparkContext(sconf)
    val rdd = t.get[Long,(String,Long,Int),(Long,String,Long,Boolean)]("hdfs://slave1.htdata.com:8020/chen/checkpoint",sc).foreach(x=>{})
  }
  @Test
  def main(): Unit = {
    System.setProperty("HADOOP_USER_NAME","hdfs")
    val t = new HTStateRddAquireFromCheckPoint
    /*val p = t.getRddPath("hdfs://slave1.htdata.com:8020/chen/checkpoint")
    println(p)
    val rddP = t.getSpecificRddPath(p)
    println(rddP)*/
    val sconf = new SparkConf().setMaster("local[4]").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer").setAppName("socket").registerKryoClasses(Array(classOf[DPList], classOf[DPListWithDN],classOf[Array[DPUnion]],
      classOf[DPUnion],classOf[MetricWithDN],classOf[com.htiiot.resources.utils.DeviceNumber],classOf[Properties],classOf[scala.collection.mutable.WrappedArray.ofRef[_]],classOf[MapWithStateRDDRecord[_,_,_]],
      classOf[OpenHashMapBasedStateMap[_,_]]))
    val sc = new SparkContext(sconf)
    val rdd = t.get[Long,(String,Long,Int),(Long,String,Long,Boolean)]("hdfs://slave1.htdata.com:8020/chen/checkpoint",sc).foreach(x=>{})
//    EmptyRDD.get[Long,(String,Long,Int)](sc).foreach(x=>{})
  }
  @Test
  def test{
    println(new HTStateRddAquireFromCheckPoint getRddListPath("hdfs://slave1.htdata.com:8020/chen/checkpoint"))
  }
}
