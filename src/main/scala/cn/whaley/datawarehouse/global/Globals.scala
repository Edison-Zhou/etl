package cn.whaley.datawarehouse.global

/**
  * Created by Tony on 17/4/5.
  */
object Globals {

  val HDFS_BASE_PATH = "/data_warehouse"
  val DIMENSION_HDFS_BASE_PATH: String = HDFS_BASE_PATH + "/dw_dimensions"
  val FACT_HDFS_BASE_PATH: String = HDFS_BASE_PATH + "/dw_facts"
  val NORMALIZED_TABLE_HDFS_BASE_PATH: String = HDFS_BASE_PATH + "/dw_normalized"
  val DM_HDFS_BASE_PATH : String = HDFS_BASE_PATH + "/dw_dm"
  val DM_FACT_HDFS_BASE_PATH : String = DM_HDFS_BASE_PATH + "/event"
  val DM_DIMENSION_HDFS_BASE_PATH : String = DM_HDFS_BASE_PATH + "/dim"


}
