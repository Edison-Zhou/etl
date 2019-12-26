package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB

/**
 * Created by huanghu on 17/11/21.
 * wui版本号和OTA版本的对应关系
 */
object Wui extends DimensionBase{

  columns.skName = "wui_sk"
  columns.primaryKeys = List("wui")
  columns.trackingColumns = List()
  columns.allColumns = List("wui","ota")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "wui" ->"wui",
    "ota" -> "ota"
  )

  sourceFilterWhere = null
  sourceDb = MysqlDB.dwDimensionDb("whaley_wui")

  dimensionName = "dim_whaley_wui"

}
