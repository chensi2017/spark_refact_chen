package org.apache.spark.streaming.rdd

import java.net.URI

import com.iiot.stream.rdd.EmptyRDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.ReliableCheckpointRDD
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * @author chensi
  */
class HTStateRddAquire {
  @transient val logger = Logger.getLogger(classOf[HTStateRddAquire])

  /**
    * 获取状态RDD初始值
    * !!!切记调用此方法一定要在ssc.checkpoint("...")之前
    * egg:
    *   new HTStateRddAquire().get("",sc)
    *   ssc.checkpoint("")
    * @param checkpointAddr
    * @param sc
    * @tparam K
    * @tparam S
    * @tparam E
    * @return
    */
  def  get[K,S,E](checkpointAddr: String,sc:SparkContext) = {
    var rddPath:Path = null
    try {
      //get rdd dir path
      val rddDirPath = getRddPath(checkpointAddr)
      //get the specific rdd path
      rddPath = getSpecificRddPath(rddDirPath)
    }catch {
      case e:Exception=>logger.error(s"checkpoint地址解析异常,获取状态rdd失败,准备创建空白状态rdd...\r\n${e}")
    }
    if(rddPath!=null){
      val path = rddPath.toString
      logger.info(s"正在从checkpoint(${path})中获取状态RDD...")
      new ReliableCheckpointRDD[MapWithStateRDDRecord[K,S,E]](sc,path)
        .mapPartitions(iter=>{
          val b = new ArrayBuffer[(K,S)]
          iter.foreach(x=>{
            val iter = x.stateMap.getAll()
            if(!iter.isEmpty){
              while (iter.hasNext){
                val item = iter.next()
                b.+=((item._1,item._2))
              }
            }
          })
          b.iterator
        })
    }else{
      logger.info("checkpoint中没有状态数据...")
      EmptyRDD.get[K,S](sc)
    }
  }

  /**
    * 寻找特定rdd目录
    * @param rddDirPath
    * @return
    */
  def getSpecificRddPath(rddDirPath:Path) ={
    val conf = new Configuration()
    val fs = FileSystem.get(URI.create(rddDirPath.toString),conf)
    val rddStatus = fs.listStatus(rddDirPath)
    val map = new mutable.HashMap[Long,FileStatus]()
    rddStatus.foreach(x=>{
      if(fs.listStatus(x.getPath).size!=(35+1)){
        map.put(x.getModificationTime,x)
      }
    })
    map.get(getLargestKey(map.keySet)).get.getPath
  }

  /**
    * 获取checkpoint下最新存放rdd的文件夹路径
    * @param checkpointAddr
    * @return
    */
  def getRddPath(checkpointAddr: String) = {
    val conf = new Configuration()
    val fs = FileSystem.get(URI.create(checkpointAddr),conf)
    val status = fs.listStatus(new Path(checkpointAddr))
    val map = new mutable.HashMap[Long,FileStatus]()
    status.foreach(x=>{
      if(x.isDirectory&&(!x.getPath.getName.startsWith("received"))){
        map.put(x.getModificationTime,x)
      }
    })
    val dir = map.get(getLargestKey(map.keySet)).get
    dir.getPath
  }

  def getLargestKey(keySet:scala.collection.Set[Long]) ={
    var largestKey = 0l
    keySet.foreach(x=>{
      if(x>largestKey){
        largestKey = x
      }
    })
    largestKey
  }

}
