package cn.whaley.datawarehouse.fact.whaley.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
  * Created by huanghu on 17/7/27.
  * 判断关于HDFS文件相关的处理
  */
object HdfsUtil {

  /**
    * 判断hdfs中是否存在所需要的文件
    *
    * @return true or false
    */

  def isHDFSFileExist(file: String): Boolean = {
    //val file = DataIO.getDataFrameOps.getPath(pattern,logType,date)
    val conf = new Configuration()
    val fs = FileSystem.get(conf)

    val path = new Path(file)
    fs.exists(path)
  }


}
