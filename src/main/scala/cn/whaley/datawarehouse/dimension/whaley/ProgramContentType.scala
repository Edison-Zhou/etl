package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB

/**
  * Created by zhangyu on 17/7/31
 *  content_type的维度表
  */
object ProgramContentType extends DimensionBase {

  columns.skName = "program_content_type_sk"
  columns.primaryKeys = List("program_content_type_id")
  columns.trackingColumns = List()
  columns.allColumns = List(
    "program_content_type_id",
    "content_type",
    "content_type_name")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "program_content_type_id" -> "id"
  )


  sourceDb = MysqlDB.dwDimensionDb("program_content_type")

  dimensionName = "dim_program_content_type"
}
