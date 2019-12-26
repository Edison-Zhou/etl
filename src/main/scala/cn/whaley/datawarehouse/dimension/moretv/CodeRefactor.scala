package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB


/**
  * Created by Chubby on 17/3/8.
  *
  * 路径中的code与站点树中的code的映射维度表
  */
object CodeRefactor extends DimensionBase {
  columns.skName = "medusa_path_program_site_map_sk"
  columns.primaryKeys = List("path_code")
  columns.trackingColumns = List()
  columns.allColumns = List("path_code", "program_code")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "path_code"->"path_code",
    "program_code" -> "program_code"
  )

  sourceFilterWhere = "path_code is not null and path_code <>''"
  sourceDb = MysqlDB.dwDimensionDb("medusa_path_code_program_code_map")

  dimensionName = "medusa_path_program_site_code_map"
}
