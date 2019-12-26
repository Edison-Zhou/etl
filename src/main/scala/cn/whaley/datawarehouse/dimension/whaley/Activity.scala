package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB

/**
 * Created by zhangyu on 17/3/14.
 * 活动维度表
 */
object Activity extends DimensionBase{

  columns.skName = "activity_sk"
  columns.primaryKeys = List("activity_sid")
  columns.trackingColumns = List()
  columns.allColumns = List("activity_sid", "activity_name")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "activity_sid" -> "sid",
    "activity_name" -> "title"
  )

  sourceFilterWhere = "activity_sid is not null and activity_sid <> ''"
  sourceDb = MysqlDB.mergerActivity

  dimensionName = "dim_whaley_activity"

  fullUpdate = true
}
