package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB

/**
  * 创建人：郭浩
  * 创建时间：2017/5/12
  * 程序作用：
  * 数据输入：
  * 数据输出：
  * 音乐榜单维度表
  */
object MvHotList extends DimensionBase {

  dimensionName = "dim_whaley_mv_hot_list"

  columns.skName = "mv_hot_sk"

  columns.primaryKeys = List("mv_hot_id")

  columns.allColumns = List(
  "mv_hot_id", "mv_hot_rank_id", "mv_hot_code", "mv_hot_name", "mv_hot_year", "mv_hot_week_code"
  )


  readSourceType = jdbc

  sourceColumnMap = Map(
  "mv_hot_id" -> "id",
  "mv_hot_rank_id" -> "parseRankId(code, year, weekCode)",
  "mv_hot_code" -> "code",
  "mv_hot_name" -> "name",
  "mv_hot_year" -> "year",
  "mv_hot_week_code" -> "weekCode"
  )

  sourceDb = MysqlDB.whaleyCms("mtv_mv_hotList", "id", 1, 3000, 10)

  sourceFilterWhere = "mv_hot_code is not null and trim(mv_hot_code) <> ''"

  sourceTimeCol = "publish_time"

  override def beforeExecute(): Unit = {
  sqlContext.udf.register("parseRankId", parseRankId _)
}

  def parseRankId(code: String, year: Int, weekCode: Int):String = {
  if (code == null || code.trim == "") {
  null
} else {
  code + "_" + year + "_" + weekCode
}
}

}
