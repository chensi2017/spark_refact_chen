package org.apache.spark

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.{Decoder, StringDecoder}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.streaming.kafka.{HTKafkaUtils, HasOffsetRanges, KafkaCluster}
import scala.reflect.ClassTag
import scala.util.control.Breaks.breakable

/**
  * @author chensi
  * @param kafkaParams
  */
class KafkaManager(val kafkaParams:Map[String,String]) extends Serializable {
  private var kc = new KafkaCluster(kafkaParams)

  /**
    * 创建数据流
    */
  def createDirectStream[K: ClassTag,
  V: ClassTag,
  KD <: Decoder[K]: ClassTag,
  VD <: Decoder[V]: ClassTag](ssc: StreamingContext, topics: Set[String]) =  {
    val groupId = kafkaParams.get("group.id").get
    // 在kafka上读取offsets前先根据实际情况更新offsets
    setOrUpdateOffsets(topics, groupId)

    //从kafka上读取offset开始消费message
    val messages = {
      val partitionsE = kc.getPartitions(topics)
      if (partitionsE.isLeft)
        throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")
      val partitions = partitionsE.right.get
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions,1)
      if (consumerOffsetsE.isLeft)
        throw new SparkException(s"get kafka consumer offsets failed: ${consumerOffsetsE.left.get}")
      val consumerOffsets = consumerOffsetsE.right.get
      HTKafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
        ssc, kafkaParams, consumerOffsets, (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message))
    }
    messages
  }

  /**
    * 创建数据流前，根据实际消费情况更新消费offsets
    * @param topics
    * @param groupId
    */
   private def setOrUpdateOffsets(topics:Set[String],groupId:String)={
    topics.foreach(topic=>{
      var hasConsmed = true

      val partitionsE = kc.getPartitions(Set(topic))
      if(partitionsE.isLeft) {
        throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")
      }
      val partitions = partitionsE.right.get
      val consumerOffsetE = kc.getConsumerOffsets(groupId,partitions,1)
      //以下是测试代码
      consumerOffsetE.right.get.foreach(x=>{
        println(s"${x._1}:::::${x._2}")
      })
      breakable {
        consumerOffsetE.right.get.foreach(x => {
          //if not consumed,the offsets is -1
          if (x._2 == -1) {
            hasConsmed = false
            import scala.util.control.Breaks._
            break
          }
        })
      }
      if(hasConsmed){//消费过
        println(s"groupid:${groupId}消费过.....")
        /**
          * 如果streaming程序执行的时候出现kafka.common.OffsetOutOfRangeException，
          * 说明zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除。
          * 针对这种情况，只要判断一下zk上的consumerOffsets和earliestLeaderOffsets的大小，
          * 如果consumerOffsets比earliestLeaderOffsets还小的话，说明consumerOffsets已过时,
          * 这时把consumerOffsets更新为earliestLeaderOffsets
          */
        val earliestLeaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
        if(earliestLeaderOffsetsE.isLeft)throw  new SparkException(s"get earliest leader offsets failed: ${earliestLeaderOffsetsE.left.get}")
        val earliestLeaderOffsets = earliestLeaderOffsetsE.right.get
        val consumerOffsets = consumerOffsetE.right.get
        // 可能只是存在部分分区consumerOffsets过时，所以只更新过时分区的consumerOffsets为earliestLeaderOffsets
        var offsets:Map[TopicAndPartition,Long] = Map()
        consumerOffsets.foreach({case(tp,n)=>
            val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
            if(n<earliestLeaderOffset){
              println("consumer group:" + groupId + ",topic:" + tp.topic + ",partition:" + tp.partition +
                " offsets已经过时，更新为" + earliestLeaderOffset)
              offsets += ( tp -> earliestLeaderOffset)
            }
        })
        if(!offsets.isEmpty){
          kc.setConsumerOffsets(groupId,offsets,1)
        }
      }else{//没有消费过
        println("该groupid没有消费过...")
        val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase())
        var leaderOffsets:Map[TopicAndPartition,LeaderOffset] = null
        if(reset == Some("smallest")){
          println("kafka的设置为smallest...")
          val leaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
          if(leaderOffsetsE.isLeft)throw new SparkException(s"get earliest leader offsets failed: ${leaderOffsetsE.left.get}")
          leaderOffsets = leaderOffsetsE.right.get
        }else{
          println("kakfa的设置为largest...")
          val leaderOffsetE = kc.getLatestLeaderOffsets(partitions)
          if(leaderOffsetE.isLeft)throw new SparkException(s"get largest leader offsets failed: ${leaderOffsetE.left.get}")
          leaderOffsets = leaderOffsetE.right.get
        }
        val offsets = leaderOffsets.map{
          case(tp,offset)=>(tp,offset.offset)
        }
        offsets.foreach(x=>println(s"${x._1}:::::::::${x._2}:::::::${groupId}"))
        var r = submitOffset(groupId,offsets)
        if(r.isLeft){
          println("wrong!!!"+r.left.get)
        } else {
          println("update offsets success!!!")
        }
      }
    })
  }

  def submitOffset(groupId:String,topicAndPartition: Map[TopicAndPartition,Long]) ={
    kc.setConsumerOffsets(groupId,topicAndPartition,1)
  }


  /**
    * 更新zookeeper/kafka上的消费offsets
    * VersionApi:0->kafka;1->zookeeper
    * @param rdd
    */
  def updateOffsets(rdd: RDD[(String, String)]) : Unit = {
    val groupId = kafkaParams.get("group.id").get
    val offsetsList = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    for (offsets <- offsetsList) {
      val topicAndPartition = TopicAndPartition(offsets.topic, offsets.partition)
      println(s"${topicAndPartition}::${offsets.untilOffset}::${groupId}")
      val start = System.currentTimeMillis()
      val o = kc.setConsumerOffsets(groupId, Map((topicAndPartition, offsets.untilOffset)),1)
      val end = System.currentTimeMillis()
//      print(s"====提交offset耗时:${end-start}ms====")
      if (o.isLeft) {
        println(s"Error updating the offset to Kafka cluster: ${o.left.get}")
      }
    }
  }

}