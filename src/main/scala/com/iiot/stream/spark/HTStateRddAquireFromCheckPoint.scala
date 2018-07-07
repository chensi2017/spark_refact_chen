package org.apache.spark.streaming.rdd

import java.io.FileNotFoundException
import java.net.URI

import com.iiot.stream.rdd.EmptyRDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.{RDD, ReliableCheckpointRDD}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * @author chensi
  */
class HTStateRddAquireFromCheckPoint {
  @transient val logger = Logger.getLogger(classOf[HTStateRddAquireFromCheckPoint])

  /**
    * 获取状态RDD初始值
    * !!!切记调用此方法一定要在ssc.checkpoint("...")之前
    * egg:
    * new HTStateRddAquire().get("",sc)
    *   ssc.checkpoint("")
    *
    * @param checkpointAddr
    * @param sc
    * @tparam K
    * @tparam S
    * @tparam E
    * @return
    */
  /*def  get[K,S,E](checkpointAddr: String,sc:SparkContext) = {
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
  }*/

  def get[K, S, E](checkpointAddr: String, sc: SparkContext):RDD[(K,S)] = {
    //get rdd dir path list
    var rddDirPath:ArrayBuffer[(FileStatus,Long)] = new ArrayBuffer[(FileStatus, Long)]()
    try {
      rddDirPath = getRddListPath(checkpointAddr)
    }catch {
      case e:FileNotFoundException=>{
        logger.error(s"checkpoint地址:${checkpointAddr}不存在!准备创建准备创建空状态RDD...\r\n${e}")
        return sc.emptyRDD[(K,S)]
      }
    }
    //iterate rdd dir path to find the correct path
    var count = -1
    for (path <- rddDirPath) {
      count = count+1
      //get the specific rdd path
      val p = path._1.getPath
      val rddPathL = getSpecificRddPathList(p)
      var flag = true
      for(rddPath<-rddPathL if flag) {
        val path = rddPath.toString
        try {
          //          val rddPath = getSpecificRddPath(p)
          logger.info(s"正在从checkpoint(${path})中获取状态RDD...")
          val rdd = new ReliableCheckpointRDD[MapWithStateRDDRecord[K, S, E]](sc, path)
            .mapPartitions(iter => {
              val b = new ArrayBuffer[(K, S)]
              iter.foreach(x => {
                val iter = x.stateMap.getAll()
                if (!iter.isEmpty) {
                  while (iter.hasNext) {
                    val item = iter.next()
                    b.+=((item._1, item._2))
                  }
                }
              })
              b.iterator
            })
          rdd.foreach(x => {})
          logger.info(s"checkpoint地址:${path}|数据恢复成功!!!")
          //clear useless hdfs state dir
          rddDirPath.remove(count)
          removeHdfsDir(rddDirPath, checkpointAddr)
          return rdd
        } catch {
          case e: Exception => {
            logger.error(s"该checkpoint:${path}地址无状态rdd数据,正在从下个一地址进行恢复!\r\n${e}")
          }
        }
      }
    }
    logger.info("所有的checkpoint地址内无有效状态数据,准备创建空状态RDD...")
    return sc.emptyRDD[(K,S)]
  }

  private def removeHdfsDir(tuples: ArrayBuffer[(FileStatus, Long)],checkpointAddr:String): Unit ={
    try {
      val conf = new Configuration()
      val fs = FileSystem.get(URI.create(checkpointAddr),conf)
      logger.info("正在清除hdfs文件夹...")
      tuples.foreach(t => {
        fs.delete(t._1.getPath, true)
      })
    }catch {
      case e:Exception=>logger.error(e)
    }
  }

  /**
    * 寻找特定rdd目录
    *
    * @param rddDirPath
    * @return
    */
  def getSpecificRddPath(rddDirPath: Path, partitionNum: Int = 35) = {
    val conf = new Configuration()
    val fs = FileSystem.get(URI.create(rddDirPath.toString), conf)
    val rddStatus = fs.listStatus(rddDirPath)
    val map = new mutable.HashMap[Long, FileStatus]()
    rddStatus.foreach(x => {
      if (fs.listStatus(x.getPath).size != (partitionNum + 1)) {
        map.put(x.getModificationTime, x)
      }
    })
    map.get(getLargestKey(map.keySet)).get.getPath
  }

  /**
    * 返回符合规定的rdd目录集合,按时间降序排序
    * @param rddDirPath
    * @param partitionNum
    * @return
    */
  def getSpecificRddPathList(rddDirPath: Path, partitionNum: Int = 35) = {
    val list = new ArrayBuffer[Path]()
    val fs = FileSystem.get(URI.create(rddDirPath.toString), new Configuration())
    val rddStatus = fs.listStatus(rddDirPath)
    val map = new mutable.HashMap[Long, FileStatus]()
    rddStatus.foreach(x => {
      if (fs.listStatus(x.getPath).size != (partitionNum + 1)) {
        map.put(x.getModificationTime, x)
      }
    })
    map.keySet.toList.sortWith((a,b)=>{a>b}).foreach(x=>{
      list += map.get(x).get.getPath
    })
    list
  }

  /**
    * 获取checkpoint下最新存放rdd的文件夹路径
    *
    * @param checkpointAddr
    * @return
    */
  def getRddPath(checkpointAddr: String) = {
    val map = getAllRddPath(checkpointAddr)
    val dir = map.get(getLargestKey(map.keySet)).get
    dir.getPath
  }

  /**
    * 获取checkpoint下所有存放rdd的文件夹路径按时间降序
    *
    * @param checkpointAddr
    * @return
    */
  def getRddListPath(checkpointAddr: String) = {
    val list = new ArrayBuffer[(FileStatus, Long)]()
    val map = getAllRddPath(checkpointAddr)
    val keyArr = map.keySet.toArray.sortWith((a, b) => (a > b)).foreach(x => {
      list.+=((map.get(x).get, x))
    })
    list
  }

  private def getAllRddPath(checkpointAddr: String) = {
    val conf = new Configuration()
    val fs = FileSystem.get(URI.create(checkpointAddr), conf)
    val status = fs.listStatus(new Path(checkpointAddr))
    val map = new mutable.HashMap[Long, FileStatus]()
    status.foreach(x => {
      if (x.isDirectory && (!x.getPath.getName.startsWith("received"))) {
        map.put(x.getModificationTime, x)
      }
    })
    map
  }

  private def getLargestKey(keySet: scala.collection.Set[Long]) = {
    var largestKey = 0l
    keySet.foreach(x => {
      if (x > largestKey) {
        largestKey = x
      }
    })
    largestKey
  }

}
