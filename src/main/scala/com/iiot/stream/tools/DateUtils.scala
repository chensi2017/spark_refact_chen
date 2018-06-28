package com.iiot.stream.tools

import java.text.SimpleDateFormat
import java.util.Date

object DateUtils {

  def formateTimeStamp(ts:Long): Int ={
    new SimpleDateFormat("yyyyMMdd").format(new Date(ts)).toInt
  }

  /**
    * 判断某一个时间戳是否是今天
    * @param ts
    * @return true:是今天 false不是今天
    */
  def isToday(ts:Long):Boolean={
    val sdf = new SimpleDateFormat("yyyyMMdd")
    sdf.format(new Date).equals(sdf.format(new Date(ts)))
  }
}
