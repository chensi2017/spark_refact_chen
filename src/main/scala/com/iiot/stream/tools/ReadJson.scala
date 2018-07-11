package com.iiot.stream.tools

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.reflect.ClassTag

object ReadJson {
  private val mappper = new ObjectMapper().registerModule(DefaultScalaModule)
  def readValue[T:ClassTag](json:String, valueType:Class[T]): T ={
    mappper.readValue[T](json,valueType)
  }
}
