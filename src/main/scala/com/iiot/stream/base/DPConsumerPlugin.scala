package com.iiot.stream.base

import com.htiiot.resources.model.ThingBullet
import com.htiiot.store.model.DataPoint
import com.htiiot.stream.plugin.IDataStreamConsumer

/**
  * Created by yuxiao on 2017/7/5.
  * the implement of IDSConsumerPlugin
  * it will load the jar pakage from "interface servers" by using RESTFul.(in the method "getPlugin")
  */
object DPConsumerPlugin extends  IDataStreamConsumer{
 /* var config: IConfig = new MemConfig("pulgin_interface.properties")
  var jarUrl: String = config.get("spark.plugin.url")
  var className: String = config.get("spark.plugin.class")*/

//  val zkClent = new ZookeeperClient
//  val zk = zkClent.getConfingFromZk("192.168.0.77:2181", 30000)
//  var config = zkClent.getAll(zk, "/conf_htiiot/pulgin_interface")
//  var jarUrl: String = config.getProperty("spark.plugin.url")
//  var className: String = config.getProperty("spark.plugin.class")
//
//  var jcl: JarClassLoader = _
  var pulgin: com.htiiot.stream.plugin.IDataStreamConsumer = _
//  var iDataStreamConsumer  = new DataStreamConsumer()
  var iDataStreamConsumer  = new DataStreamConsumerRefact()
//  init()
//
//   def init(): Unit = {
//    jcl = new JarClassLoader
//    jcl.add(new URL(jarUrl))
//  }

  def getPlugin() = {
    /*if (null == pulgin) {
      var factory = JclObjectFactory.getInstance
      var obj = factory.create(jcl, className)
      if (null == obj || !obj.isInstanceOf[com.htiiot.stream.plugin.IDataStreamConsumer]) {
        pulgin = null
      }
      pulgin = obj.asInstanceOf[com.htiiot.stream.plugin.IDataStreamConsumer]
    }
    this.pulgin*/
    iDataStreamConsumer
  }

  override def checkDataPoint(componentId: Long, metricName: String, metricValue: Double): Array[Long] = {
    if (null == pulgin) {
      return null
    }
    pulgin.checkDataPoint(componentId, metricName, metricValue)
  }

  override def checkDataPointToThingBullet(point: DataPoint): Array[ThingBullet] = {
    if (null == pulgin) {
      return null
    }
    pulgin.checkDataPointToThingBullet(point)
  }

  override def checkDataPoint(point: DataPoint): Array[Long] = {
    if (null == pulgin) {
      return null
    }
    pulgin.checkDataPoint(point)
  }

  /** * 插件销毁时调用此接口。用来处理销毁一些变量等 */
   def destroy(): Unit = {}

  override def getVersion(): Long = {
    1L
  }



/*object DPConsumerPlugin {
  def main(args: Array[String]): Unit = {
    var plugin = new DPConsumerPlugin
    val dn =new DeviceNumber(NumberTools.intToBytes(10),NumberTools.intToBytes(10),NumberTools.longToBytes(10L))
    val dp =new DataPoint()
    dp.setDeviceNumber(dn)
    dp.setTs(10L)
    dp.setMetric(new Metric("test",10))
    plugin.getPlugin()
    plugin.checkDataPointToThingBullet(dp)
  }*/
}