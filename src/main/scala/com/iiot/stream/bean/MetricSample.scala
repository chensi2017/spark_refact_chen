package com.iiot.stream.bean

class MetricSample {
  var ts: Long = 0L
  var k: String = ""
  var v: String = ""


  def getTs: Long = ts
  def setTs(value: Long): Unit = {
    ts = value
  }


  def getK: String = k
  def setK(key: String): Unit = {
    k = key
  }


  def getV: String = v
  def setV(value: String): Unit = {
    v = value
  }

}
