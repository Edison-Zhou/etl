package cn.whaley.datawarehouse.market.dim.constants

import cn.whaley.datawarehouse.global.Globals.DM_DIMENSION_HDFS_BASE_PATH

/**
  * @author wangning
  * @date 2019/9/6 15:38
  */
object Constants {
  val DM_DIMENSION_HDFS_BASE_PATH_BACKUP: String = DM_DIMENSION_HDFS_BASE_PATH + "/backup"
  val DM_DIMENSION_HDFS_BASE_PATH_TMP: String = DM_DIMENSION_HDFS_BASE_PATH + "/tmp"
  val DM_DIMENSION_HDFS_BASE_PATH_DELETE: String = DM_DIMENSION_HDFS_BASE_PATH + "/delete"
  val DM_THRESHOLD_VALUE = 512000

}
