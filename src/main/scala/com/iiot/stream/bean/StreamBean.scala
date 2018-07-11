package com.iiot.stream.bean

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.htiiot.resources.utils.DeviceNumber

case class DPUnion(_dn: DeviceNumber, _ts: Long, _tid: String, _metricId: Long, _thingId: String,  _key:String,_value:Double, _dnStr: String, var threshold:String)
@JsonIgnoreProperties(ignoreUnknown = true)
case class DPListWithDN(ts: Long,tid: Long,did: Long,data:Array[MetricWithDN])
case class MetricWithDN(ts:Long,k:String,v:String,dn:String)