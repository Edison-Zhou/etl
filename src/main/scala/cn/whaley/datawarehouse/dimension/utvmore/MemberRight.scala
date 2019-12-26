package cn.whaley.datawarehouse.dimension.utvmore

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType.jdbc
import cn.whaley.datawarehouse.util.MysqlDB

/**
  * Created by xia.jun on 2017/11/9.
  */
object MemberRight extends DimensionBase{


  dimensionName = "dim_utvmore_member_right"

  columns.skName = "member_right_sk"

  columns.primaryKeys = List("account_id", "member_code")

  columns.allColumns = List(
    "account_id", "account_name", "member_code", "member_name", "start_time", "effective_time", "status", "create_time"
  )


  readSourceType = jdbc

  sourceColumnMap = Map(
    columns.primaryKeys(0) -> "account_id",
    columns.allColumns(1) -> "account_name",
    columns.allColumns(2) -> "member_code",
    columns.allColumns(3) -> "member_name",
    columns.allColumns(4) -> "start_time",
    columns.allColumns(5) -> "effective_time",
    columns.allColumns(6) -> "status",
    columns.allColumns(7) -> "create_time"
  )

  sourceDb = MysqlDB.utvmoreMemberDB("member_user_authority")

  sourceFilterWhere = "account_id is not null and account_id <> ''"



}
