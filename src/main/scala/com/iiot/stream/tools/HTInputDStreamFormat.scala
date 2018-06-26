package com.iiot.stream.tools

import com.alibaba.fastjson.JSON
import com.iiot.stream.bean.{DPList, DPListWithDN, DPUnion}
import org.apache.log4j.Logger
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ArrayBuffer

/**
  * Created by liu on 2017-07-21.
  */
object HTInputDStreamFormat {
  @transient lazy val logger: Logger = Logger.getLogger(HTInputDStreamFormat.getClass)

  //
  def inputDStreamFormatWithDN(stream: DStream[(String, String)]): DStream[DPUnion] = {
    val resultJson = stream.map(x => dataTransformWithDN(JSON.parseObject(x._2,classOf[DPListWithDN])))
      .flatMap(x => x)
    resultJson.cache()
    resultJson
  }

  def inputDStreamFormatWithDNFilter(stream: DStream[(String, String)]): DStream[DPUnion] = {
    val resultJson = stream.map(x => dataTransformWithDN(
      try {
        JSON.parseObject(x._2, classOf[DPListWithDN])
      }catch {
        case e:Exception=>null
      }
    )).flatMap(x  => x).filter(x=>{
      if(x!=null)
        true
      else
        false
    })
    resultJson.cache()
    resultJson
  }

  def dataTransformWithDN(x: DPListWithDN): ArrayBuffer[DPUnion] = {
    //    logger.info("the raw string is " + x.toString)
    if(x==null){
      return null;
    }
    var result = ArrayBuffer[DPUnion]()
    try {
      var ts: Long = x.getTs
      val tid = x.getTid
      val dsn = x.getDid
      var data = x.getData
      var dn: String = "0"
      var compId = "0"
      var thingId = "0"
      for (metric <- data) {
        if (metric.getTs.toString.nonEmpty) {
          ts = metric.getTs
        }
        val metricName = metric.getK
        val metricValue = metric.getV
        dn = metric.getDN

        if ((dn != "0") && (dn != null)) {
          //compId = DeviceNumber.fromBase64String(dn).getMetricIdLong.toString
          //thingId = DeviceNumber.fromBase64String(dn).getDeviceIdInt.toString
          val dp = new DPUnion()
          //val deviceNumber = com.htiiot.resources.utils.DeviceNumber.fromBase64String(dn)
          val deviceNumber = com.htiiot.resources.utils.DeviceNumber.fromHexString(dn)
          dp.setDn(deviceNumber)
          dp.setTs(ts)
          dp.setKey(metricName)
          dp.setMetricId(deviceNumber.getComponentId.toString)
          dp.setTid(tid)
          dp.setThingId(deviceNumber.getThingId.toString)
          dp.setValue(metricValue)
          result += dp
        }
      }
    } catch {
      case e: NullPointerException => {
        logger.error("NullPointer Error" + e.getMessage)
      }
      case e: NumberFormatException => {
        logger.error("NumberFormat Error" + e.getMessage)
      }
      case e: Exception => {
        logger.error("Unknown Error" + e.getMessage)
      }
    }
    //    logger.info("the result string is :" + result)
    //    logger.info("the result string  num is :" + result.size)
    result
  }

  def inputDStreamFormat(stream: DStream[(String, String)]): DStream[DPUnion] = {
    val resultJson = stream.map(x => JSON.parseObject(x._2, classOf[DPList]))
      .map(x => dataTransform(x))
      .flatMap(x => x)
    resultJson
  }

  def dataTransform(x: DPList): ArrayBuffer[DPUnion] = {
    logger.info("the raw string is " + x.toString)
    var ts: Long = x.getTs
    val tid = x.getTid
    val dsn = x.getDsn
    var data = x.getData
    var result = ArrayBuffer[DPUnion]()
    var dn: String = "0"
    var compId = "0"
    var thingId = "0"
    for (metric <- data) {
      if (metric.getTs.toString.nonEmpty) {
        ts = metric.getTs
      }
      val metricName = metric.getK
      val metricValue = metric.getV
      val dnFromCache = "AAAAAQAAABAAAAAAAANqzg=="
      dn = dnFromCache
      if ((dn != "0") && (dn != null)) {
        //compId = DeviceNumber.fromBase64String(dn).getMetricIdLong.toString
        //thingId = DeviceNumber.fromBase64String(dn).getDeviceIdInt.toString
        val dp = new DPUnion()
        val deviceNumber = com.htiiot.resources.utils.DeviceNumber.fromBase64String(dn)
        deviceNumber.getComponentId
        deviceNumber.getThingId
        dp.setDn(deviceNumber)
        dp.setTs(ts)
        dp.setKey(metricName)
        dp.setMetricId(deviceNumber.getComponentId.toString)
        dp.setTid(tid)
        dp.setThingId(deviceNumber.getThingId.toString)
        dp.setValue(metricValue)
        result += dp
      }
    }
    logger.info("the result string is :" + result)
    logger.info("the result string  num is :" + result.size)
    result
  }

}