package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB

/**
  * Created by witnes on 3/13/17.
  * 歌手维度表
  */
object Singer extends DimensionBase {

  dimensionName = "dim_medusa_singer"

  columns.skName = "singer_sk"

  columns.primaryKeys = List("singer_id")

  columns.trackingColumns = List()

  columns.allColumns = List(
    "singer_id",
    "singer_name",
    "singer_area",
    "singer_birthday")


  readSourceType = jdbc

  sourceDb = MysqlDB.medusaCms("mtv_singer", "id", 1, 134, 1)

  sourceColumnMap = Map(
    columns.primaryKeys(0) -> "sid",
    columns.allColumns(1) -> "name",
    columns.allColumns(2) -> "area",
    columns.allColumns(3) -> "birthday"
  )

  sourceDb = MysqlDB.medusaCms("mtv_singer", "id", 1, 550, 1)

  sourceFilterWhere = "singer_id is not null and singer_id <> ''"

  sourceTimeCol = "updateTime"
}
