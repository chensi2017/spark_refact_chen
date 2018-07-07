package org.apache.spark.streaming

object StreamingContextRefact {
  def setCheckpointDuration(ssc:StreamingContext,duration:Duration)={
    ssc.checkpointDuration = duration
  }
}
