package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB


/**
  * Created by czw on 17/3/14.
  *
  * 微鲸端电台维度表
  */
object Radio extends DimensionBase {
  columns.skName = "radio_sk"
  columns.primaryKeys = List("radio_sid")
  columns.trackingColumns = List()
  columns.allColumns = List("radio_sid","radio_name")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "radio_sid" -> "sid",
    "radio_name" -> "title"
  )

  sourceFilterWhere = "radio_sid is not null and radio_sid <> ''"
  sourceDb = MysqlDB.whaleyCms("mtv_mvradio","sid",1,10000,2)

  dimensionName = "dim_whaley_radio"
}
