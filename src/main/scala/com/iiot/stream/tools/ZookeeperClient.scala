package com.iiot.stream.tools

import java.nio.charset.StandardCharsets
import java.util.Properties

import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}

class ZookeeperClient {
  @transient lazy val logger: Logger = Logger.getLogger(classOf[ZookeeperClient])
   var prop = new Properties()
  def getConfingFromZk(host: String, TimeOut: Int) = {
    var zk: ZooKeeper = null
    try {
      zk = new ZooKeeper(host, TimeOut, new Watcher {
        override def process(event: WatchedEvent): Unit = {
         logger.info("Already triggered " + event.getType() + " event !!!")
        }
      })
    } catch {
      case e: Exception => logger.error("zk lost",e)
    }
    zk
  }

  def create(zk: ZooKeeper, path: String,node:String,data :String) ={
    if (zk != null && path !=null && data !=null) {
      val stat = zk.exists(path, false)
      if (stat == null) {
        zk.create(path, node.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
        zk.setData(path,data.getBytes,-1)
        println(new String(zk.getData(path,false,null)))
      }else{
        logger.info(path+" is exists!!!")
        println(new String(zk.getData(path,false,null)))
      }
    }
  }

  def getAll(zk: ZooKeeper, path: String) = {

    if (zk != null) {
      // val prop = new Properties()
      try {
        val stat = zk.exists(path, false)
        if (stat != null) {
          var childrenList: List[String] = null
          childrenList = zk.getChildren(path, false).toList

          for (child <- childrenList) {
            var childPath = path
            if (!childPath.endsWith("/")) {
              childPath = childPath + "/"
            }

            childPath = childPath + child
            var buffer = zk.getData(childPath, true, stat)
            if (buffer != null) {
              var value = new String(buffer, StandardCharsets.UTF_8)
              if (value != null) prop.setProperty(child.toString.trim, value.trim)
              println("("+child+ ": "+ prop.get(child)+")")
            }
          }
        }
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    prop
  }
}