package cn.whaley.datawarehouse.market.event.constant

import cn.whaley.datawarehouse.global.Globals.DM_FACT_HDFS_BASE_PATH

/**
  * @author wangning
  * @date 2019/9/5 15:25
  */
object Constants {
  val DM_HDFS_BASE_PATH_BACKUP: String = DM_FACT_HDFS_BASE_PATH + "/backup"
  val DM_HDFS_BASE_PATH_TMP: String = DM_FACT_HDFS_BASE_PATH + "/tmp"
  val DM_HDFS_BASE_PATH_DELETE: String = DM_FACT_HDFS_BASE_PATH + "/delete"
  val DM_FACT_HDFS_BASE_PATH_COMPLETE: String = DM_FACT_HDFS_BASE_PATH + "/completeSource"
  val DM_FACT_HDFS_BASE_PATH_COMPLETE_TMP: String = DM_FACT_HDFS_BASE_PATH_COMPLETE + "/tmp"
  val THRESHOLD_VALUE = 5120000
  val DM_THRESHOLD_VALUE = 640000
}
