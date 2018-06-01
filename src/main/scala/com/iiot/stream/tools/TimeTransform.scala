package com.iiot.stream.tools

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by liu on 2017-07-12.
  */
object TimeTransform {
  def timestampToDate(timestamp: Long): String = {
    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val date: String = simpleDateFormat.format(new Date(timestamp))
    date
  }
}
