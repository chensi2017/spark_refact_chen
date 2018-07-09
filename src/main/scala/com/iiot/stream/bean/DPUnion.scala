package com.iiot.stream.bean

import com.htiiot.resources.utils.DeviceNumber

case class DPUnion(_dn: DeviceNumber, _ts: Long, _tid: String, _metricId: Long, _thingId: String,  _key:String,_value:Double, _dnStr: String, var threshold:String)
