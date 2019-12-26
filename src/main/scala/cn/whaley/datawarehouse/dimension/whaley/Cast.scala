package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB


/**
  * Created by lituo.
  *
  * 微鲸端演员维度表
  */
object Cast extends DimensionBase {
  columns.skName = "cast_sk"
  columns.primaryKeys = List("cast_sid")
  columns.trackingColumns = List()
  columns.allColumns = List("cast_sid", "cast_name", "cast_name_pinyin", //"cast_other_name",
  "gender", "constellation", "birthday")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "cast_sid" -> "sid",
    "cast_name" -> "name",
    "cast_name_pinyin" -> "namePinyin",
//    "cast_other_name" -> "case when name1 is null or trim(name1) = '' then name else name1 end",
    "gender" -> "sex"
  )

  sourceFilterWhere = "cast_sid is not null and cast_sid <> ''"
  sourceDb = MysqlDB.whaleyCms("mtv_cast", "sid", 140000, 4000000, 10)

  dimensionName = "dim_whaley_cast"
}
