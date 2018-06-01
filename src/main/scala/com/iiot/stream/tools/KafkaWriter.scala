package com.iiot.stream.tools

import java.util.Properties

import com.fasterxml.jackson.databind.ObjectMapper
import com.htiiot.resources.model.ThingBullet
import kafka.common.KafkaException
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.log4j.Logger

/**
  * Created by yuxiao on 2017/9/15.
  * it is a single pattern.all the kafka operator use the only object.
  */

object KafkaWriter {
  lazy val logger: Logger = Logger.getLogger("KafkaWriter")
  //kf1.inter.htiiot.com
  var TOPIC:String = _
  //should be a output topic
  var producer: KafkaProducer[String,String] = _


  def kafkaInit(brokerList:String,topic:String): Unit = {
    val startTime = System.currentTimeMillis();
    val props = new Properties()
    props.put("metadata.broker.list", brokerList)
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put("producer.type","async")
    producer = new KafkaProducer[String, String](props)
    val endTime = System.currentTimeMillis()
    println(s"[KafkaWriter]init KafkaProducer time: ${endTime-startTime}")
  }

  def getResource(brokerList:String,topic:String): Unit ={
    if(producer == null) {
      synchronized {
        if(producer == null) {
          TOPIC = topic
          kafkaInit(brokerList, topic)
        }
      }
    }
    producer
  }

  def writer(thingBullets: Array[ThingBullet]): Unit = {
    if(thingBullets == null){
      //To Do something
    }else {
      for (thingBullet <- thingBullets) {
        //should follow the rules of kafka's event.
        val mapper = new ObjectMapper
        try {
          val jsonString: String = mapper.writeValueAsString(thingBullet)
          producer.send(new ProducerRecord(TOPIC, "bullet", jsonString))
//          println("the bullet string is : " + jsonString)
        }catch {
          case e:KafkaException =>{
            logger.error(s"event write to kafka is fault\r\n${e}")
          }
        }
      }
    }
  }

  def destory() = {
    producer.close()
  }

}
