package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB


/**
  * Created by Chubby on 17/3/8.
  *
  * 电视猫推荐位维度表
  */
object Position extends DimensionBase {
  columns.skName = "position_sk"
  columns.primaryKeys = List("position_code")
  columns.trackingColumns = List()
  columns.allColumns = List("position_code", "position_title", "position_content_type","type","status")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "position_code" -> "code",
    "position_title"->"title",
    "position_content_type" -> "name",
    "type"->"type",
    "status"->"status"
  )

  sourceFilterWhere = "position_code is not null and position_code <> ''"
  sourceDb = MysqlDB.medusaCms("mtv_position", "id", 1, 2010000000, 1)

  dimensionName = "dim_medusa_position"
}
