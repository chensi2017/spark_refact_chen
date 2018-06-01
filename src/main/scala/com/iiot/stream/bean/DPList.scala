package com.iiot.stream.bean


class DPList {
  var ts: Long = 0l
  var tid: String = ""
  var dsn: String =""
  var data = Array[MetricSample]()


  def setData(list : Array[MetricSample]): Unit ={
    data = list
  }
  def getData :Array[MetricSample] ={
    data
  }



  def getDsn: String = dsn
  def setDsn(value: String): Unit = {
    dsn = value
  }


  def getTs: Long = ts
  def setTs(value: Long): Unit = {
    ts = value
  }


  def getTid: String = tid
  def setTid(value: String): Unit = {
    tid = value
  }

  override def toString():String ={
    var str = ts + dsn + tid + "data[....]"
    for(i <- data){
      str  = str + "key:"+ i.getK + "ts :"+  i.getTs +  "value" +i.getV
    }
    str
  }

}
