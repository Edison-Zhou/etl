package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame

/**
  * Created by witnes on 3/13/17.
  * 音乐精选集维度表
  */
object MVTopic extends DimensionBase {

  dimensionName = "dim_medusa_mv_topic"

  columns.skName = "mv_topic_sk"

  columns.primaryKeys = List("mv_topic_sid")

  columns.trackingColumns = List()

  columns.allColumns = List(
    "mv_topic_sid",
    "mv_topic_name",
    "mv_status",
    "mv_topic_create_time",
    "mv_topic_publish_time"
  )



  readSourceType = jdbc

  sourceColumnMap = Map(
    columns.primaryKeys(0) -> "sid",
    columns.allColumns(1) -> "title",
    columns.allColumns(2) -> "status",
    columns.allColumns(3) -> "cast(create_time as timestamp)",
    columns.allColumns(4) -> "cast(publish_time as timestamp)"
  )

  sourceDb = MysqlDB.medusaCms("mtv_mvtopic", "id", 1, 134, 1)

  //临时过滤34fhwxac9wtv防止报错
  sourceFilterWhere = "mv_topic_sid is not null and mv_topic_sid <> '' and mv_status=1"

  override def filterSource(sourceDf: DataFrame): DataFrame = {
    super.filterSource(sourceDf).dropDuplicates(columns.primaryKeys)
  }
}
