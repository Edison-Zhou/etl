package cn.whaley.datawarehouse.normalized.medusa

import java.io.File
import java.util.{Calendar, Date}

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.dimension.constant.Constants.{NORMALIZED_TABLE_HDFS_BASE_PATH_BACKUP, NORMALIZED_TABLE_HDFS_BASE_PATH_DELETE, NORMALIZED_TABLE_HDFS_BASE_PATH_TMP, THRESHOLD_VALUE}
import cn.whaley.datawarehouse.fact.constant.LogPath
import cn.whaley.datawarehouse.global.Globals.NORMALIZED_TABLE_HDFS_BASE_PATH
import cn.whaley.datawarehouse.util.{DataExtractUtils, DateFormatUtils, HdfsUtil, Params}
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

/**
  * Created by xia.jun on 2017/11/7.
  * 该类用于绑定注册账号后第一次使用的uid信息
  */
object BindUidToAccount extends BaseClass{

  override def extract(params: Params) = {

    params.paramMap.get("date") match {
      case Some(p) => {
        params.mode match {
          case "all" => {
            val startDate = DateFormatUtils.readFormat.parse("20170101")
            val endDate = DateFormatUtils.readFormat.parse(p.toString)
            val pathDate = getPathDate(startDate, endDate)
            val accountLoginDF = DataExtractUtils.readFromParquet(sqlContext, LogPath.MEDUSA_ACCOUNT_LOGIN, pathDate)
            val accountDF = DataExtractUtils.readFromParquet(sqlContext, LogPath.MEDUSA_ACCOUNT)
            getUid(accountDF, accountLoginDF)
          }
          case "increment" => {
            val previousAccountUidMap = DataExtractUtils.readFromParquet(sqlContext, LogPath.MEDUSA_ACCOUNT_UID_MAP)
            val currentAccountDF = DataExtractUtils.readFromParquet(sqlContext, LogPath.MEDUSA_ACCOUNT)
            val currentAccountLoginDF = DataExtractUtils.readFromParquet(sqlContext, LogPath.MEDUSA_ACCOUNT_LOGIN, params.startDate)
            val needUpdateAccountDF = previousAccountUidMap.filter("user_id is null").select("account_id").
              union(currentAccountDF.select("account_id").except(previousAccountUidMap.select("account_id")))
            getUid(needUpdateAccountDF, currentAccountLoginDF).union(previousAccountUidMap.filter("user_id is not null"))
          }
        }
      }
      case None => throw new RuntimeException("未设置时间参数！")
    }
  }

  override def transform(params: Params, df: DataFrame) = {
    df.groupBy("account_id").agg("user_id" -> "min").withColumnRenamed("min(user_id)","user_id")
  }


  override def load(params: Params, df: DataFrame):Unit = {
    backup(params,df,"medusa_account_uid_mapping")
  }

  /**
    * 用来备份维度数据，然后将维度数据生成在临时目录，当isOnline参数为true的时候，将临时目录的数据替换线上维度
    *
    * @param p  the main args
    * @param df the DataFrame from execute function
    * @return a Unit.
    */
  private def backup(p: Params, df: DataFrame, dimensionType: String): Unit = {

    val cal = Calendar.getInstance
    val date = DateFormatUtils.readFormat.format(cal.getTime)
    val onLineNormizedDir = NORMALIZED_TABLE_HDFS_BASE_PATH + File.separator + dimensionType
    val onLineNormizedBackupDir = NORMALIZED_TABLE_HDFS_BASE_PATH_BACKUP + File.separator + date + File.separator + dimensionType
    val onLineNormizedDirTmp = NORMALIZED_TABLE_HDFS_BASE_PATH_TMP + File.separator + dimensionType
    val onLineNormizedDirDelete = NORMALIZED_TABLE_HDFS_BASE_PATH_DELETE + File.separator + dimensionType
    println("线上数据目录:" + onLineNormizedBackupDir)
    println("线上数据备份目录:" + onLineNormizedBackupDir)
    println("线上数据临时目录:" + onLineNormizedDirTmp)
    println("线上数据等待删除目录:" + onLineNormizedDirDelete)

    df.persist(StorageLevel.MEMORY_AND_DISK)
    val isOnlineFileExist = HdfsUtil.IsDirExist(onLineNormizedDir)
    if (isOnlineFileExist) {
      val isBackupExist = HdfsUtil.IsDirExist(onLineNormizedBackupDir)
      if (isBackupExist) {
        println("数据已经备份,跳过备份过程")
      } else {
        println("生成线上维度备份数据:" + onLineNormizedBackupDir)
        val isSuccessBackup = HdfsUtil.copyFilesInDir(onLineNormizedDir, onLineNormizedBackupDir)
        println("备份数据状态:" + isSuccessBackup)
      }
    } else {
      println("无可用备份数据")
    }

    //防止文件碎片
    val total_count = BigDecimal(df.count())
    val partition = Math.max(1, (total_count / THRESHOLD_VALUE).intValue())
    println("repartition:" + partition)

    val isTmpExist = HdfsUtil.IsDirExist(onLineNormizedDirTmp)
    if (isTmpExist) {
      println("删除线上维度临时数据:" + onLineNormizedDirTmp)
      HdfsUtil.deleteHDFSFileOrPath(onLineNormizedDirTmp)
    }
    println("生成线上维度数据到临时目录:" + onLineNormizedDirTmp)
    df.repartition(partition).write.parquet(onLineNormizedDirTmp)

    println("数据是否上线:" + p.isOnline)
    if (p.isOnline) {
      println("数据上线:" + onLineNormizedDir)
      if (isOnlineFileExist) {
        println("移动线上维度数据:from " + onLineNormizedDir + " to " + onLineNormizedDirDelete)
        val isRenameSuccess = HdfsUtil.rename(onLineNormizedDir, onLineNormizedDirDelete)
        println("isRenameSuccess:" + isRenameSuccess)
      }

      val isOnlineFileExistAfterRename = HdfsUtil.IsDirExist(onLineNormizedDir)
      if (isOnlineFileExistAfterRename) {
        throw new RuntimeException("rename failed")
      } else {
        val isSuccess = HdfsUtil.rename(onLineNormizedDirTmp, onLineNormizedDir)
        println("数据上线状态:" + isSuccess)
      }
      println("删除过期数据:" + onLineNormizedDirDelete)
      HdfsUtil.deleteHDFSFileOrPath(onLineNormizedDirDelete)
    }

  }

  /**
    * 匹配account_id 与 uid
    * @param accountDF
    * @param accountLoginDF
    * @return
    */
  def getUid(accountDF:DataFrame, accountLoginDF:DataFrame):DataFrame = {
    val accountFirstLoginDF = accountDF.join(accountLoginDF, accountLoginDF("accountId") === accountDF("account_id")).
      groupBy("account_id").agg("datetime" -> "min").withColumnRenamed("min(datetime)","datetime").distinct()
    val accountUidDF = accountFirstLoginDF.join(accountLoginDF, accountLoginDF("accountId") === accountFirstLoginDF("account_id") &&
      accountLoginDF("datetime") === accountFirstLoginDF("datetime")).select(accountFirstLoginDF("account_id"),
      accountLoginDF("userId")).distinct()
    val df = accountDF.join(accountUidDF, accountDF("account_id") === accountUidDF("account_id"),"left_outer").
      select(accountDF("account_id"),accountUidDF("userId")).withColumnRenamed("userId","user_id")
    df
  }

  /**
    * 提取日志路径中的日期信息
    * @param startDate
    * @param endDate
    * @return
    */
  def getPathDate(startDate:Date, endDate:Date):String = {
    var dateArr = ListBuffer[String]()
    val dateDiffs = (endDate.getTime - startDate.getTime) / (1000*3600*24)
    val calendar = Calendar.getInstance()
    calendar.setTime(startDate)
    (0 to dateDiffs.toInt).foreach(i => {
      dateArr.+=(DateFormatUtils.readFormat.format(calendar.getTime))
      calendar.add(Calendar.DAY_OF_MONTH, 1)
    })
    "{" + dateArr.mkString(",") + "}"
  }

}
