package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB


/**
  * Created by czw on 17/3/14.
  *
  * 微鲸端直播维度表
  */
object Live extends DimensionBase {
  columns.skName = "live_sk"
  columns.primaryKeys = List("live_sid")
  columns.trackingColumns = List()
  columns.allColumns = List("live_sid","live_channel","live_channel_code")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "live_sid" -> "sid",
    "live_channel" -> "station",
    "live_channel_code" -> "station_code"
  )

  sourceFilterWhere = "live_sid is not null and live_sid <> ''"
  sourceDb = MysqlDB.whaleyCms("mtv_channel","sid",1,100000,5)

  dimensionName = "dim_whaley_live"
}
