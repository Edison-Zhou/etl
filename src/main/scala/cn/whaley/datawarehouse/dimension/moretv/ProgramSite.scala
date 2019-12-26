package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB


/**
  * Created by Chubby on 17/3/8.
  *
  * 电视猫菜单树维度表
  */
object ProgramSite extends DimensionBase {
  columns.skName = "program_site_sk"
  columns.primaryKeys = List("program_site_id")
  columns.trackingColumns = List()
  columns.allColumns = List("program_site_id", "site_code", "site_name", "site_content_type", "template_code", "site_sub_title","status",
                              "parent_id","type")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "program_site_id" -> "id",
    "site_code" -> "code",
    "site_name" -> "name",
    "site_content_type" -> "contentType",
    "template_code"->"templateCode",
    "site_sub_title"->"subTitle",
    "status"->"status",
    "parent_id"->"parentId",
    "type"->"type"
  )

  sourceFilterWhere = "program_site_id is not null"
  sourceDb = MysqlDB.medusaCms("mtv_program_site", "id", 1, 2010000000, 10)

  dimensionName = "dim_medusa_program_site"

  fullUpdate = true
}
