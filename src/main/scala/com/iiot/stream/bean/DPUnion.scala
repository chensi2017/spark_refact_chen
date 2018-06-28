package com.iiot.stream.bean

import com.htiiot.resources.utils.DeviceNumber

class DPUnion  extends Serializable {
  var _dn: DeviceNumber = _
  var _ts: Long =0L
  var _tid: String = ""
  var _metricId: Long = _
  var _thingId: String = ""

  var _value:String= ""
  var _key:String= ""
  var _dnStr: String = ""


  def getKey: String = _key
  def setKey(value: String): Unit = {
    _key = value
  }

  def getValue: String = _value
  def setValue(value: String): Unit = {
    _value = value
  }

  def getDnStr: String = _dnStr
  def setDnStr(value: String): Unit = {
    _dnStr = value
  }


  def getDn: DeviceNumber = _dn
  def setDn(value: DeviceNumber): Unit = {
    _dn = value
  }


  def getTs: Long= _ts
  def setTs(value: Long): Unit = {
    _ts = value
  }


  def getTid: String = _tid
  def setTid(value: String): Unit = {
    _tid = value
  }


  def getMetricId: Long = _metricId
  def setMetricId(value: Long): Unit = {
    _metricId = value
  }


  def getThingId: String = _thingId
  def setThingId(value: String): Unit = {
    _thingId = value
  }


  override def toString = "DPUnion----()"+_metricId
}