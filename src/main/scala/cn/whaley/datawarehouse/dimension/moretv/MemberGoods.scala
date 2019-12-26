package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType.jdbc
import cn.whaley.datawarehouse.util.MysqlDB

/**
  * Created by xia.jun on 2017/11/9.
  */
object MemberGoods extends DimensionBase{


  dimensionName = "dim_medusa_member_goods"

  columns.skName = "good_sk"

  columns.primaryKeys = List("good_code","good_price")

  columns.allColumns = List(
    "good_code", "good_name", "good_price", "duration", "member_code", "member_name", "is_valid"
  )


  readSourceType = jdbc

  sourceColumnMap = Map(
    columns.primaryKeys(0) -> "good_code",
    columns.allColumns(1) -> "good_name",
    columns.allColumns(2) -> "good_price",
    columns.allColumns(3) -> "duration",
    columns.allColumns(4) -> "member_code",
    columns.allColumns(5) -> "member_name",
    columns.allColumns(6) -> "is_valid"
  )

  sourceDb = MysqlDB.dwDimensionDb("medusa_member_goods")

  sourceFilterWhere = "good_code is not null and good_name <> ''"



}
