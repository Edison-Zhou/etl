package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.util.MysqlDB

/**
  * Created by LuoZiyu on 17/06/23.
  */
@Deprecated
object DangerousCityLevel extends DimensionBase {
  private val tableName = "mtv_dgcity"

  columns.skName = "city_dangerous_level_sk"
  columns.primaryKeys = List("city_id")
  columns.trackingColumns = List()
  columns.allColumns = List("city_id", "city_name", "dangerous_level", "judge")

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    columns.primaryKeys(0) -> "id",
    columns.allColumns(1) -> "cityName",
    columns.allColumns(2) -> "level_city"
  )

  sourceDb = MysqlDB.medusaProgramInfo(tableName)
  dimensionName = "dim_medusa_city_dangerous_level"

  sourceFilterWhere = "judge = 1"

  fullUpdate = true

}
