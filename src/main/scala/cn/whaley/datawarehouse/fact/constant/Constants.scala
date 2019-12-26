package cn.whaley.datawarehouse.fact.constant

import cn.whaley.datawarehouse.global.Globals._

/**
  * Created by michael on 17/5/5.
  */
object Constants {
  val FACT_HDFS_BASE_PATH_BACKUP: String = FACT_HDFS_BASE_PATH + "/backup"
  val FACT_HDFS_BASE_PATH_TMP: String = FACT_HDFS_BASE_PATH + "/tmp"
  val FACT_HDFS_BASE_PATH_DELETE: String = FACT_HDFS_BASE_PATH + "/delete"
  val FACT_HDFS_BASE_PATH_CHECK: String = FACT_HDFS_BASE_PATH + "/check"
  val FACT_HDFS_BASE_PATH_COMPLETE: String = FACT_HDFS_BASE_PATH + "/completeSource"
  val FACT_HDFS_BASE_PATH_COMPLETE_TMP: String = FACT_HDFS_BASE_PATH_COMPLETE + "/tmp"
  val THRESHOLD_VALUE = 5120000
  val FACT_THRESHOLD_VALUE = 640000

}
