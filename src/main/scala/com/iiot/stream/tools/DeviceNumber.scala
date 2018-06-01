package com.iiot.stream.tools

import java.nio.ByteBuffer
import java.util
import java.util.Base64

/**
  * Created by liu on 2017-07-31.
  */

class  DeviceNumber extends Serializable {
  val DEVICE_NUMBER_SIZE = 16 //16 bytes
  val USER_ID_SIZE = 4
  val THING_ID_SIZE = 4
  val COMPONENT_ID_SIZE = 8
  private val dnBytes = new Array[Byte](DEVICE_NUMBER_SIZE)
  protected val hexArray: Array[Char] = "0123456789ABCDEF".toCharArray

  def getDnBytes: Array[Byte] = dnBytes

  def toBase64String = new String(Base64.getEncoder.encode(dnBytes))

  def toHexString: String = {
    val hexChars = new Array[Char](dnBytes.length * 2)
    for (i <- dnBytes.indices) {
      val v = dnBytes(i) & 0xFF
      hexChars(i * 2) = hexArray(v >>> 4)
      hexChars(i * 2 + 1) = hexArray(v & 0x0F)
    }
    //    for (i <- 0 until dnBytes.length) {
    //      val v = dnBytes(i) & 0xFF
    //      hexChars(i * 2) =  hexArray(v >>> 4)
    //      hexChars(i * 2 + 1) = hexArray(v & 0x0F)
    //    }
    new String(hexChars)
  }

  def this(uId: Array[Byte], thingId: Array[Byte], componentId: Array[Byte]) {
    this
    if (uId.length != USER_ID_SIZE ||
      thingId.length != THING_ID_SIZE ||
      componentId.length != COMPONENT_ID_SIZE) {
      throw new IllegalArgumentException("input id length invalid")
    }
    System.arraycopy(uId, 0, dnBytes, 0, USER_ID_SIZE)
    System.arraycopy(thingId, 0, dnBytes, 0 + USER_ID_SIZE, THING_ID_SIZE)
    System.arraycopy(componentId, 0, dnBytes, 0 + USER_ID_SIZE + THING_ID_SIZE, COMPONENT_ID_SIZE)
  }

  def this(tenantId: Int, thingId: Int, componentId: Long) {
    this(ByteBuffer.allocate(4).putInt(tenantId).array(),
      ByteBuffer.allocate(4).putInt(thingId).array(),
      ByteBuffer.allocate(8).putLong(componentId).array())
  }

  def this(dnBytes: Array[Byte]) {
    this
    if (dnBytes.length != DEVICE_NUMBER_SIZE) {
      throw new IllegalArgumentException("input bytes length invalid")
    }
    System.arraycopy(dnBytes, 0, this.dnBytes, 0, DEVICE_NUMBER_SIZE)
  }

  def getTenantId: Array[Byte] = util.Arrays.copyOfRange(dnBytes, 0, USER_ID_SIZE)

  def getTenantIdInt: Int = NumberTools.bytesToInt(getTenantId)

  def getDeviceId: Array[Byte] = util.Arrays.copyOfRange(dnBytes, 0 + USER_ID_SIZE, USER_ID_SIZE + THING_ID_SIZE)

  def getDeviceIdInt: Int = NumberTools.bytesToInt(getDeviceId)

  def getMetricId: Array[Byte] = util.Arrays.copyOfRange(dnBytes, 0 + USER_ID_SIZE + THING_ID_SIZE, USER_ID_SIZE + THING_ID_SIZE + COMPONENT_ID_SIZE)

  def getMetricIdLong: Long = NumberTools.bytesToLong(getMetricId)
}

object DeviceNumber {
  def fromBase64String(base64Str: String): DeviceNumber = {
    val base64Bytes: Array[Byte] = base64Str.getBytes()
    new DeviceNumber(Base64.getDecoder.decode(base64Bytes))
  }

  def fromHexString(hexStr: String): DeviceNumber = {
    val len = hexStr.length()
    val data: Array[Byte] = new Array[Byte](len / 2)
    for (i <- 0 until len if i % 2 == 0) {
      data(i / 2) = ((Character.digit(hexStr.charAt(i), 16) << 4) + Character.digit(hexStr.charAt(i + 1), 16)).toByte
    }
    new DeviceNumber(data)
  }
}





