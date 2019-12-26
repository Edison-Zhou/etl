package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}
import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.util.MysqlDB

/**
  * Created by Tony on 17/3/10.
  *
  * 电视猫用户登陆维度表
  */
object TerminalUserLogin extends DimensionBase {

  columns.skName = "user_login_sk"
  columns.primaryKeys = List("user_id")
  columns.trackingColumns = List()
  columns.allColumns = List(
    "user_id",
    "mac",
    "last_login_time"
  )

  sourceColumnMap = Map(
    "last_login_time" -> "lastLoginTime"
  )

  sourceFilterWhere = "user_id is not null and user_id <> ''"
  sourceDb = MysqlDB.medusaTvServiceAccount

  columns.linkDimensionColumns = List(
    new DimensionColumn(
      "dim_medusa_terminal_user",
      List(DimensionJoinCondition(Map("user_id" -> "user_id"))),
      "user_sk"
    )
  )

  dimensionName = "dim_medusa_terminal_user_login"

  sourceTimeCol = "lastLoginTime"

}
