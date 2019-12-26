package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB


/**
  * Created by Chubby on 17/3/8.
  *
  * 电视猫菜单树类型维度表
  */
object ProgramSiteType extends DimensionBase {
  columns.skName = "program_site_type_sk"
  columns.primaryKeys = List("program_site_type_code")
  columns.trackingColumns = List()
  columns.allColumns = List("program_site_type_code", "program_site_type_name", "status")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "program_site_type_code" -> "code",
    "program_site_type_name" -> "name",
    "status"->"status"
  )

  sourceFilterWhere = "program_site_type_code is not null and program_site_type_code <> ''"
  sourceDb = MysqlDB.medusaCms("mtv_program_site_type", "id", 1, 2010000000, 1)

  dimensionName = "dim_medusa_program_site_type"

  fullUpdate = true
}
