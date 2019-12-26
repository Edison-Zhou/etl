package cn.whaley.datawarehouse.dimension.constant

import cn.whaley.datawarehouse.global.Globals._

/**
  * Created by Tony on 17/3/8.
  */
object Constants {

  val DIMENSION_HDFS_BASE_PATH_BACKUP: String = DIMENSION_HDFS_BASE_PATH + "/backup"
  val DIMENSION_HDFS_BASE_PATH_TMP: String = DIMENSION_HDFS_BASE_PATH + "/tmp"
  val DIMENSION_HDFS_BASE_PATH_DELETE: String = DIMENSION_HDFS_BASE_PATH + "/delete"
  val THRESHOLD_VALUE = 512000

  val NORMALIZED_TABLE_HDFS_BASE_PATH_BACKUP: String = NORMALIZED_TABLE_HDFS_BASE_PATH + "/backup"
  val NORMALIZED_TABLE_HDFS_BASE_PATH_TMP: String = NORMALIZED_TABLE_HDFS_BASE_PATH + "/tmp"
  val NORMALIZED_TABLE_HDFS_BASE_PATH_DELETE: String = NORMALIZED_TABLE_HDFS_BASE_PATH + "/delete"

}
