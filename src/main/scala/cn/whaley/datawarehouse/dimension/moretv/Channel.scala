package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB

/**
  * Created by witnes on 3/13/17.
  * 直播频道维度表
  */
object Channel extends DimensionBase {

  dimensionName = "dim_medusa_channel"

  columns.skName = "channel_sk"

  columns.primaryKeys = List("channel_sid")

  columns.allColumns = List(
    "channel_sid", "channel_name", "channel_code", "channel_source_name", "channel_create_time", "channel_publish_time"
  )


  readSourceType = jdbc

  sourceColumnMap = Map(
    columns.primaryKeys(0) -> "sid",
    columns.allColumns(1) -> "station",
    columns.allColumns(2) -> "station_code",
    columns.allColumns(3) -> "source",
    columns.allColumns(4) -> "cast(create_time as timestamp)",
    columns.allColumns(5) -> "cast(publish_time as timestamp)"
  )

  sourceDb = MysqlDB.medusaCms("mtv_channel", "id", 1, 134, 1)

  sourceFilterWhere = "channel_sid is not null and channel_sid <> ''"

  sourceTimeCol = "publish_time"

}
