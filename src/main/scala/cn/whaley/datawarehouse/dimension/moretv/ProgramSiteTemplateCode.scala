package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB


/**
  * Created by Chubby on 17/3/8.
  *
  * 电视猫菜单树模板Code维度表
  */
object ProgramSiteTemplateCode extends DimensionBase {
  columns.skName = "program_site_template_code_sk"
  columns.primaryKeys = List("program_site_template_id")
  columns.trackingColumns = List()
  columns.allColumns = List("program_site_template_id", "program_site_template_code",
    "program_site_template_name", "status")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "program_site_template_id"->"id",
    "program_site_template_code" -> "code",
    "program_site_template_name" -> "name",
    "status"->"status"
  )

  sourceFilterWhere = "program_site_template_id is not null"
  sourceDb = MysqlDB.medusaCms("mtv_program_site_templateCode", "id", 1, 2010000000, 1)

  dimensionName = "dim_medusa_program_site_template_code"

  fullUpdate = true
}
