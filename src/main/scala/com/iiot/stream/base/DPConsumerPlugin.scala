package com.iiot.stream.base

object DPConsumerPlugin{
  val iDataStreamConsumer  = new DataStreamConsumerRefact()

  def getPlugin() = {
    iDataStreamConsumer
  }

}