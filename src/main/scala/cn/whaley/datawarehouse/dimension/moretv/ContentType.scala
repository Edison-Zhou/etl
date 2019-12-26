package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB


/**
  * Created by witnes on 3/13/17.
  * 节目类型维度表
  */
object ContentType extends DimensionBase {

  dimensionName = "dim_medusa_content_type"

  columns.skName = "content_type_sk"

  columns.primaryKeys = List("content_type_code")

  columns.allColumns = List("content_type_code", "content_type_name")


  readSourceType = jdbc

  sourceColumnMap = Map(
    columns.primaryKeys(0) -> "code",
    columns.allColumns(1) -> "name"
  )

  sourceDb = MysqlDB.medusaCms("mtv_content_type", "code", 1, 100, 1)

  sourceFilterWhere = "content_type_code is not null and content_type_code <> ''"

}
