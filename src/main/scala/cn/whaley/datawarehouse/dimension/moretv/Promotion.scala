package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB


/**
  * Created by Tony on 17/3/8.
  *
  * 电视猫推广渠道维度表
  */
object Promotion extends DimensionBase {
  columns.skName = "promotion_sk"
  columns.primaryKeys = List("promotion_code")
  columns.trackingColumns = List()
  columns.allColumns = List("promotion_code", "promotion_name", "promotion_type")

  readSourceType = jdbc

  sourceColumnMap = Map(
  )

  sourceDb = MysqlDB.dwDimensionDb("moretv_promotion")

  dimensionName = "dim_medusa_promotion"
}
