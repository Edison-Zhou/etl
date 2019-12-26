package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB


/**
  * Created by Chubby on 17/3/8.
  *
  * 电视猫固定位维度表
  */
object FixEntrance extends DimensionBase {
  columns.skName = "medusa_fix_entrance_sk"
  columns.primaryKeys = List("code")
  columns.trackingColumns = List()
  columns.allColumns = List("code", "name")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "code"->"code",
    "name" -> "name"
  )

  sourceFilterWhere = "code is not null and code <>''"
  sourceDb = MysqlDB.dwDimensionDb("medusa_fix_area_info")

  dimensionName = "dim_medusa_fix_entrance_info"
}
