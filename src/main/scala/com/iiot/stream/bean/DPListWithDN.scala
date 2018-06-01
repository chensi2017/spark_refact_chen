package com.iiot.stream.bean

class DPListWithDN {
  var ts: Long = 0l
  var tid: Long = 0l
  var did: Long = 0l
  var data = Array[MetricWithDN]()



  def setData(list : Array[MetricWithDN]): Unit ={
    data = list
  }
  def getData :Array[MetricWithDN] ={
    data
  }



  def getDid: String = did.toString
  def setDid(value:Long): Unit = {
    did = value
  }


  def getTs: Long = ts
  def setTs(value: Long): Unit = {
    ts = value
  }


  def getTid: String = tid.toString
  def setTid(value: Long): Unit = {
    tid = value
  }

  override def toString():String ={
    var str = ts + did + tid + "data[....]"
    for(i <- data){
      str  = str + "key:"+ i.getK + "ts :"+  i.getTs +  "value" +i.getV
    }
    str
  }

}
