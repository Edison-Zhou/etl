package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.util.MysqlDB
import cn.whaley.datawarehouse.global.SourceType._
/**
  * Created by Chubby on 3/13/17.
  * 节假日信息
  */
object HolidayDetail extends DimensionBase {

  dimensionName = "dim_holiday_detail"

  columns.skName = "day_sk"

  columns.primaryKeys = List("dim_day_id")

  columns.allColumns = List(
    "dim_day_id", "dim_day_info", "dim_name", "dim_type_info"
  )


  readSourceType = jdbc

  sourceColumnMap = Map(
    columns.primaryKeys(0) -> "id",
    columns.allColumns(1) -> "day",
    columns.allColumns(2) -> "name",
    columns.allColumns(3) -> "type_info"
  )

  sourceDb = MysqlDB.dwDimensionDb("holiday_detail")

  sourceFilterWhere = "dim_day_info is not null and dim_day_info <> ''"


}
