package org.apache.spark.streaming.kafka

import kafka.common.{ErrorMapping, TopicAndPartition}
import kafka.javaapi.consumer.SimpleConsumer
import kafka.javaapi.{TopicMetadata, TopicMetadataRequest}
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.{StreamingContext, Time}
import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.control.Breaks._

class HTDirectKafkaInputDStream[
K: ClassTag,
V: ClassTag,
U <: Decoder[K]: ClassTag,
T <: Decoder[V]: ClassTag,
R: ClassTag](
              @transient ssc_ : StreamingContext,
              val HTkafkaParams: Map[String, String],
              val HTfromOffsets: Map[TopicAndPartition, Long],
              messageHandler: MessageAndMetadata[K, V] => R
) extends DirectKafkaInputDStream[K, V, U, T, R](ssc_, HTkafkaParams , HTfromOffsets, messageHandler) {
  private val logger = Logger.getLogger("HTDirectKafkaInputDStream")
  logger.setLevel(Level.INFO)
  private val kafkaBrokerList:String = HTkafkaParams.get("metadata.broker.list").get
  override def compute(validTime: Time) : Option[KafkaRDD[K, V, U, T, R]] = {
    /**
      * 在这更新 currentOffsets 从而做到自适应上游 partition 数目变化
      */
    updateCurrentOffsetForKafkaPartitionChange()
    super.compute(validTime)
  }

  private def updateCurrentOffsetForKafkaPartitionChange() : Unit = {
    val topic = currentOffsets.head._1.topic
    val nextPartitions : Int = getTopicMeta(topic) match {
      case Some(x) => x.partitionsMetadata.size()
      case _ => 0
    }
    val currPartitions = currentOffsets.keySet.size

    if (nextPartitions > currPartitions) {
      var i = currPartitions
      while (i < nextPartitions) {
        currentOffsets = currentOffsets + (TopicAndPartition(topic, i) -> 0)
        i = i + 1
      }
    }
    logger.info(s"kafka add partitions|kafkaNowPartitions:${nextPartitions}|currentParttions:${currentOffsets.keySet.size}")
  }

  private def getTopicMeta(topic: String) : Option[TopicMetadata] = {
    var metaData : Option[TopicMetadata] = None
    var consumer : Option[SimpleConsumer] = None

    val topics = List[String](topic)
    val brokerList = kafkaBrokerList.split(",")
    brokerList.foreach(
      item => {
        val hostPort = item.split(":")
        try {
          breakable {
            for (i <- 0 to 3) {
              consumer = Some(new SimpleConsumer(host = hostPort(0), port = hostPort(1).toInt,
                soTimeout = 10000, bufferSize = 64 * 1024, clientId = "leaderLookup"))
              val req : TopicMetadataRequest = new TopicMetadataRequest(topics.asJava)
              val resp = consumer.get.send(req)

              metaData = Some(resp.topicsMetadata.get(0))
              if (metaData.get.errorCode == ErrorMapping.NoError) break()
            }
          }
        } catch {
          case e:Exception => logInfo(s"Error in HTDirectKafkaInputDStream ${e}")
        }
      }
    )
    metaData
  }
}
