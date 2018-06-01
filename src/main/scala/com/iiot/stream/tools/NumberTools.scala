package com.iiot.stream.tools

import java.nio.ByteBuffer

/**
  * Created by liu on 2017-07-26.
  */
object NumberTools {
  def bytesToInt(b: Array[Byte]): Int = {
    val buffer: ByteBuffer = ByteBuffer.allocate(4)
    buffer.put(b)
    buffer.flip
    buffer.getInt
  }

  def intToBytes(x: Int): Array[Byte] = {
    val buffer: ByteBuffer = ByteBuffer.allocate(4)
    buffer.putInt(x)
    buffer.array
  }

  def longToBytes(x: Long): Array[Byte] = {
    val buffer: ByteBuffer = ByteBuffer.allocate(8)
    buffer.putLong(x)
    buffer.array
  }

  def bytesToLong(bytes: Array[Byte]): Long = {
    val buffer: ByteBuffer = ByteBuffer.allocate(8)
    buffer.put(bytes)
    buffer.flip
    buffer.getLong
  }
}
