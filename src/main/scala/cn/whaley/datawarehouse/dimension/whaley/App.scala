package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB

/**
 * Created by zhangyu on 17/3/15.
 * 应用名维度表
 */
object App extends DimensionBase{

  columns.skName = "application_sk"
  columns.primaryKeys = List("application_apk")
  columns.trackingColumns = List()
  columns.allColumns = List("application_apk", "application_name")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "application_apk" -> "pack",
    "application_name" -> "name"
  )

  sourceFilterWhere = "application_apk is not null and application_apk <> ''"
  sourceDb = MysqlDB.whaleyApp("app","pack",1,460,2)

  dimensionName = "dim_whaley_application"

  fullUpdate = true
}
