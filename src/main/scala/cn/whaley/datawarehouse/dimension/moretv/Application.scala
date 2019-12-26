package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB

/**
  * Created by witnes on 3/13/17.
  * 推荐应用维度表
  */
object Application extends DimensionBase {

  dimensionName = "dim_medusa_application"

  columns.skName = "application_sk"

  columns.primaryKeys = List("application_sid")

  columns.trackingColumns = List()

  columns.allColumns = List(
    "application_sid",
    "application_name",
    "application_version",
    "application_version_name"
  )


  readSourceType = jdbc

  sourceDb = MysqlDB.medusaCms("mtv_application", "id", 1, 134, 1)

  sourceColumnMap = Map(
    columns.primaryKeys(0) -> "sid",
    columns.allColumns(1) -> "title",
    columns.allColumns(2) -> "version",
    columns.allColumns(3) -> "version_name"
  )

  sourceFilterWhere = "application_sid is not null and application_sid <> ''"


}
