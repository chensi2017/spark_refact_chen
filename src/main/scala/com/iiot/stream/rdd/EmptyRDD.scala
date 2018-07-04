package com.iiot.stream.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

/**
  * 此类用于创建空的RDD
  * @author chensi
  */
class EmptyRDD[K,V](sc:SparkContext) extends RDD[(K,V)](sc,Nil){
  override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
    Array().iterator
  }

  override protected def getPartitions: Array[Partition] = {
    Array()
  }
}
object EmptyRDD{
  def get[K,V](sc:SparkContext)={
    new EmptyRDD[K,V](sc)
  }
}
