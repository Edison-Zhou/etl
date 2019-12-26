package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB

/**
  * Created by Tony on 17/3/31.
  */
object LauncherEntrance extends DimensionBase {

  columns.skName = "launcher_entrance_sk"
  columns.primaryKeys = List("launcher_entrance_id")
  columns.trackingColumns = List()
  columns.allColumns = List(
    "launcher_entrance_id",
    "launcher_area_code",
    "launcher_area_name",
    "launcher_location_code",
    "launcher_location_name")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "launcher_entrance_id" -> "id",
    "launcher_area_code" -> "area_code",
    "launcher_area_name" -> "area_name",
    "launcher_location_code" -> "location_code",
    "launcher_location_name" -> "location_name"
  )

  sourceDb = MysqlDB.dwDimensionDb("moretv_launcher_entrance")

  dimensionName = "dim_medusa_launcher_entrance"
}
