package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB


/**
  * Created by czw on 17/3/14.
  *
  * 微鲸端音乐精选集维度表
  */
object MvTopic extends DimensionBase {
  columns.skName = "mv_topic_sk"
  columns.primaryKeys = List("mv_topic_sid")
  columns.trackingColumns = List()
  columns.allColumns = List("mv_topic_sid","mv_topic_name")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "mv_topic_sid" -> "sid",
    "mv_topic_name" -> "title"
  )

  sourceFilterWhere = "mv_topic_sid is not null and mv_topic_sid <> ''"
  sourceDb = MysqlDB.whaleyCms("mtv_mvtopic","sid",1,10000,1)

  dimensionName = "dim_whaley_mv_topic"
}
