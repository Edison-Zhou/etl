package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.fact.constant.LogPath
import cn.whaley.datawarehouse.util.DataExtractUtils
import org.apache.spark.sql.DataFrame
import cn.whaley.datawarehouse.global.SourceType._


/**
 * Created by zhangyu on 17/5/19.
 * 创建微鲸端节目类型维度表
 */
object LinkType extends DimensionBase {

  dimensionName = "dim_whaley_link_type"

  columns.skName = "link_type_sk"

  columns.primaryKeys = List("link_type_code","link_value_code")

  columns.allColumns = List(
    "link_type_code",
    "link_type_name",
    "link_value_code",
    "link_value_name"
  )

  fullUpdate = true

  override def readSource(readSourceType: Value): DataFrame = {

    val subjectType = DataExtractUtils.readFromParquet(sqlContext,LogPath.DIM_WHALEY_SUBJECT).
    filter("dim_invalid_time is null").
    selectExpr("4 as link_type_code","'专题' as link_type_name",
    "subject_code as link_value_code","subject_name as link_value_name")

    val mvTopicType = DataExtractUtils.readFromParquet(sqlContext,LogPath.DIM_WHALEY_MV_TOPIC).
    filter("dim_invalid_time is null").
    selectExpr("11 as link_type_code","'音乐精选集' as link_type_name",
    "mv_topic_sid as link_value_code","mv_topic_name as link_value_name")


    val mvHotListType = DataExtractUtils.readFromParquet(sqlContext,LogPath.DIM_WHALEY_MV_HOT_LIST)
    .filter("dim_invalid_time is null")
    .selectExpr("19 as link_type_code","'音乐榜单' as link_type_name",
    "mv_hot_rank_id as link_value_code","mv_hot_name as link_value_name")

    val singerType = DataExtractUtils.readFromParquet(sqlContext,LogPath.DIM_WHALEY_SINGER)
      .filter("dim_invalid_time is null")
      .selectExpr("37 as link_type_code","'热门歌手' as link_type_name",
      "singer_sid as link_value_code","singer_name as link_value_name")

    val radioType = DataExtractUtils.readFromParquet(sqlContext,LogPath.DIM_WHALEY_RADIO)
      .filter("dim_invalid_time is null")
      .selectExpr("36 as link_type_code","'音乐电台' as link_type_name",
      "radio_sid as link_value_code","radio_name as link_value_name")

    val matchType = DataExtractUtils.readFromParquet(sqlContext,LogPath.DIM_WHALEY_SPOTRS_MATCH)
      .filter("dim_invalid_time is null")
      .selectExpr("16 as link_type_code","'体育赛事' as link_type_name",
        "match_sid as link_value_code","match_name as link_value_name")

    subjectType.unionAll(mvTopicType).unionAll(matchType).
    unionAll(mvHotListType).unionAll(singerType).unionAll(radioType)
  }
}
