package cn.whaley.datawarehouse.normalized

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.global.Globals._
import cn.whaley.datawarehouse.util.{HdfsUtil, Params}
import org.apache.spark.sql.DataFrame

import scala.reflect.io.File

/**
  * Created by Tony on 17/4/19.
  */
abstract class NormalizedEtlBase  extends BaseClass {

  var tableName: String = _

  val HDFS_BASE_PATH_BACKUP: String = NORMALIZED_TABLE_HDFS_BASE_PATH + "/backup"
  val HDFS_BASE_PATH_TMP: String = NORMALIZED_TABLE_HDFS_BASE_PATH + "/tmp"
  val HDFS_BASE_PATH_DELETE: String = NORMALIZED_TABLE_HDFS_BASE_PATH + "/delete"
  val THRESHOLD_VALUE = 2560000


  override def load(params: Params, df: DataFrame): Unit = {
    backup(params, df)
  }


  /**
    * 只保留当前数据和上次数据
    * @param p
    * @param df
    */
  def save(p: Params, df: DataFrame): Unit = {
    val path = NORMALIZED_TABLE_HDFS_BASE_PATH + File.separator + tableName
    HdfsUtil.deleteHDFSFileOrPath(path + File.separator + "tmp")
    df.write.parquet(path + File.separator + "tmp")
    HdfsUtil.deleteHDFSFileOrPath(path + File.separator + "backup")
    HdfsUtil.rename(path + File.separator + "current", path + File.separator + "backup")
    HdfsUtil.rename(path + File.separator +  "tmp", path + File.separator +  "current")

  }

  /**
    * 用来备份实体表数据，，当isOnline参数为true的时候，将临时目录的数据替换线上维度
    * 每天结果都会保留
    *
    * @param p  the main args
    * @param df the DataFrame from execute function
    * @return a Unit.
    */
  private def backup(p: Params, df: DataFrame): Unit = {
    if (tableName == null) return
    val date = p.paramMap("date")
    val hour = if (p.startHour == null) {
      "00"
    } else {
      p.startHour
    }
    val onLineFactDir = NORMALIZED_TABLE_HDFS_BASE_PATH + File.separator + tableName + File.separator + p.paramMap("date") + File.separator + hour
    val onLineFactParentDir = NORMALIZED_TABLE_HDFS_BASE_PATH + File.separator + tableName + File.separator + p.paramMap("date")
    val onLineFactBackupDir = HDFS_BASE_PATH_BACKUP + File.separator + tableName + File.separator + date + File.separator + hour
    val onLineFactBackupParentDir = HDFS_BASE_PATH_BACKUP + File.separator + tableName + File.separator + date
    val onLineFactDirTmp = HDFS_BASE_PATH_TMP + File.separator + tableName + File.separator + date + File.separator + hour
    println("线上数据目录:" + onLineFactDir)
    println("线上数据备份目录:" + onLineFactBackupDir)
    println("线上数据临时目录:" + onLineFactDirTmp)

    //防止文件碎片
    df.persist()
    val total_count = BigDecimal(df.count())
    val load_to_hdfs_partition = (total_count / THRESHOLD_VALUE).intValue() + 1
    println("load_to_hdfs_partition:" + load_to_hdfs_partition)

    val isTmpExist = HdfsUtil.IsDirExist(onLineFactDirTmp)
    if (isTmpExist) {
      HdfsUtil.deleteHDFSFileOrPath(onLineFactDirTmp)
    }
    println("生成线上数据到临时目录:" + onLineFactDirTmp)
    df.repartition(load_to_hdfs_partition).write.parquet(onLineFactDirTmp)


    println("数据是否准备上线:" + p.isOnline)
    if (p.isOnline) {
      val isOnlineFileExist = HdfsUtil.IsDirExist(onLineFactDir)
      println("数据上线:" + onLineFactDir)
      if (isOnlineFileExist) {
        println("生成线上备份数据:" + onLineFactBackupDir)
        HdfsUtil.deleteHDFSFileOrPath(onLineFactBackupDir)
        HdfsUtil.createDir(onLineFactBackupParentDir)
        val isSuccessBackup = HdfsUtil.rename(onLineFactDir, onLineFactBackupDir)
        println("备份数据状态:" + isSuccessBackup)
      }

      val isOnlineFileExistAfterRename = HdfsUtil.IsDirExist(onLineFactDir)
      if (isOnlineFileExistAfterRename) {
        throw new RuntimeException("rename failed")
      } else {
        val isOnLineFactParentDir = HdfsUtil.createDir(onLineFactParentDir)
        println("数据上线的父目录是否创建成功:" + isOnLineFactParentDir)
        val isSuccess = HdfsUtil.rename(onLineFactDirTmp, onLineFactDir)
        println("数据上线状态:" + isSuccess)
      }
    }
  }
}
