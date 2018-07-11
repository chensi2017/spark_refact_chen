package com.iiot.stream.tools

import com.alibaba.fastjson.JSON
import com.iiot.stream.bean.{DPListWithDN, DPUnion}
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
    val resultJson = stream.map(x => dataTransformWithDN(ReadJson.readValue(x._2,classOf[DPListWithDN])))
      .flatMap(x => x)
    resultJson
  }

  /**
    * 过滤异常json串
    */
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
    if(x==null){
      return null;
    }
    var result = ArrayBuffer[DPUnion]()
    try {
      var ts: Long = x.ts
      val tid = x.tid.toString
      var data = x.data
      var dn: String = null
      for (metric <- data) {
        if (metric.ts.toString.nonEmpty) {
          ts = metric.ts
        }
        val metricName = metric.k
        val metricValue = metric.v
        dn = metric.dn

        if ((dn != "0") && (dn != null)) {
          //compId = DeviceNumber.fromBase64String(dn).getMetricIdLong.toString
          //thingId = DeviceNumber.fromBase64String(dn).getDeviceIdInt.toString
          //val deviceNumber = com.htiiot.resources.utils.DeviceNumber.fromBase64String(dn)
          val deviceNumber = com.htiiot.resources.utils.DeviceNumber.fromHexString(dn)
          val dp = DPUnion(deviceNumber,ts,tid,deviceNumber.getComponentIdLong,deviceNumber.getThingId.toString,metricName,metricValue.toDouble.formatted("%.2f").toDouble,dn,null)
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

}