package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB


/**
  * Created by czw on 17/3/14.
  *
  * 微鲸端体育比赛维度表
  */
object SportsMatch extends DimensionBase {
  columns.skName = "match_sk"
  columns.primaryKeys = List("match_sid")
  columns.trackingColumns = List()
  columns.allColumns = List("match_sid","match_pid","match_name","match_category","match_date","match_source","league_id")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "match_sid" -> "sid",
    "match_pid" -> "pid",
    "match_name" -> "title",
    "match_category" -> "category",
    "match_source" -> "source"
  )

  sourceFilterWhere = "match_sid is not null and match_sid <> ''"
  sourceDb = MysqlDB.whaleyCms("sailfish_sport_match","sid",1,700000,10)

  dimensionName = "dim_whaley_sports_match"
}
