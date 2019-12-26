package cn.whaley.datawarehouse.dimension.share

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.util.MysqlDB

/**
  * Created by Tony on 16/12/23.
  */
object AppVersion extends DimensionBase {
  private val tableName = "moretv_app_version"

  columns.skName = "app_version_sk"
  columns.primaryKeys = List("app_version_key")
  columns.trackingColumns = List()
  columns.allColumns = List("app_version_key", "app_name", "app_en_name", "app_id", "app_series", "version", "build_time"
  , "company" , "product")


  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "app_version_key" -> "cast(id as long)",
    "app_id" -> "''",
    "company" -> "'微鲸'",
    "product" -> "''"
  )

  sourceDb = MysqlDB.dwDimensionDb(tableName)
  dimensionName = "dim_app_version"

  fullUpdate = true

//  override def execute(args: Array[String]): Unit = {
//    val jdbcDF = sqlContext.read.format("jdbc").options(MysqlDB.dwDimensionDb(tableName)).load()
//    jdbcDF.registerTempTable("moretv_app_version")
//
//    val df = sqlContext.sql("SELECT cast((id + 10000) as long) as app_version_key, app_name, app_en_name, " +
//      "'' as app_id, app_series, version, build_time,  " +
//      "'微鲸' as company, '' as product" +
//      s" from $tableName")
//
//   /* HdfsUtil.deleteHDFSFileOrPath("/data_warehouse/dw_dimensions/dim_app_version")
//    df.write.parquet("/data_warehouse/dw_dimensions/dim_app_version")*/
//    (df,dimensionType)
//  }
}
