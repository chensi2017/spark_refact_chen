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

  def formateInt2String(date:Int) ={
    val sec = date/100%100
    val thir = date%100
    val secS = if(sec<10) "0"+sec else sec.toString
    val thirS = if(thir<10)"0"+thir else thir.toString
    date/10000 + "-" +secS + "-" + thirS
  }
}
