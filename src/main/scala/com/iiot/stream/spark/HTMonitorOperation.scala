package com.iiot.stream.spark

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import com.iiot.stream.bean.{DPUnion, DataPoint, MetricImpl}
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ArrayBuffer


/**
  * Created by yuxiao on 2017/9/19.
  */
class HTMonitorOperation(sparkProBro:Broadcast[Properties]) extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(classOf[HTMonitorOperation])

  val threadNum = 4

  val foreachPartitionFunc = (it: Iterator[DPUnion]) => {
    val start = System.currentTimeMillis()
    var arrTotal = new ArrayBuffer[DataPoint]()
    val sparkPro = sparkProBro.value
    //封装DataPoint
    val startPack = System.currentTimeMillis()
    try {
      it.foreach(dpUnion=> {
        var dataPoint = new DataPoint
        dataPoint.setDeviceNumber(dpUnion.getDn)
        dataPoint.setTs(dpUnion.getTs)
        try {
          dataPoint.setMetric(new MetricImpl(dpUnion.getKey, dpUnion.getValue.toDouble.formatted("%.2f").toDouble))
        } catch {
          case e: NumberFormatException => {
            e.printStackTrace()
          }
        }
        arrTotal += dataPoint
      })
    }catch {
      case e:NullPointerException =>{
        logger.error("NullPointer Error"+e.getMessage)
      }
      case e:NumberFormatException =>{
        logger.error("NumberFormat Error"+ e.getMessage)
      }
      case e:Exception =>{
        logger.error("Unknown Error"+ e.getMessage)
      }
    }
    val endPack = System.currentTimeMillis() - startPack
    val startAllThread = System.currentTimeMillis()
    //这里的平均算法并不是很好
    try {
      var start = 0
      val step = Math.floorDiv(arrTotal.length, threadNum)
      val threadbuff = ArrayBuffer[Thread]()
      if (0 == arrTotal.length) {
      }
      else {
        for (i <- 1 to threadNum) {
          val flag = if (i == threadNum) 1 else 0
          val arrSize = step + (arrTotal.length - (threadNum) * step) * flag
          val arr = util.Arrays.copyOfRange(arrTotal.toArray,start,start+arrSize)
          start = start + step
//          val thread = new MyThread("thread" + i, i, arr, hashMap)
          val thread = new MyThread("thread" + i, i, arr, sparkPro)
          threadbuff += thread
          thread.start()
        }
        for (thread <- threadbuff) {
          thread.join()
        }
      }
    }catch {
      case e:IllegalThreadStateException =>{
        logger.error("IllegalThreadState Error"+ e.getMessage)
      }
      case e:ArrayIndexOutOfBoundsException =>{
        logger.error("ArrayIndexOutOfBounds Error"+ e.getMessage)
      }
      case e:Exception =>{
        logger.error("Unknown Error"+ e.getMessage)
      }
    }
    val endAllThread = System.currentTimeMillis() - startAllThread
    val endT = System.currentTimeMillis()
    println(s"${new SimpleDateFormat("yy-MM-dd hh:mm:ss").format(new Date())}${Thread.currentThread()}[分区监控]总时间: ${endT-start} ms;{DataUnion封装成DataPoint时间: ${endPack} ms;线程运行总耗时: ${endAllThread} ms;}")
  }


  def monitor(jsonData: DStream[DPUnion]) = {
    jsonData.foreachRDD(rdd => {
      rdd.sparkContext.setLocalProperty("spark.scheduler.pool","pool_d")
        rdd.foreachPartition(foreachPartitionFunc)
    })
  }

}

