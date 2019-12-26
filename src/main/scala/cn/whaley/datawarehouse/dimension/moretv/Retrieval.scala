package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
  * Created by Tony
  * 筛选维度表
  */
object Retrieval extends DimensionBase {

  dimensionName = "dim_medusa_retrieval"

  columns.skName = "retrieval_sk"

  columns.primaryKeys = List(
    "retrieval_key",
    "content_type")

  columns.trackingColumns = List()

  columns.allColumns = List(
    "retrieval_key",
    "sort_type",
    "filter_category_first",
    "filter_category_second",
    "filter_category_third",
    "content_type")


  readSourceType = jdbc

  sourceDb = MysqlDB.dwDimensionDb("moretv_retrieval")


  override def filterSource(sourceDf: DataFrame): DataFrame = {
    val contentTypeList = List("movie", "tv", "zongyi", "comic", "jilu", "mv")
    val dfList = contentTypeList.map(contentType => {
      val sortTypeDf = sourceDf.where(s"content = 'sort_type' and content_type in ('$contentType','all')").select("value")
        .withColumnRenamed("value", "sort_type").withColumn("fake_id", lit(1))

      val filterCategoryFirst = sourceDf.where(
        s"content = 'filter_category_first' and content_type in ('$contentType','all')"
      ).select("value").withColumnRenamed("value", "filter_category_first"
      ).withColumn("fake_id", lit(1))

      val filterCategorySecond = sourceDf.where(
        s"content = 'filter_category_second' and content_type in ('$contentType','all')"
      ).select("value").withColumnRenamed("value", "filter_category_second").withColumn("fake_id", lit(1))

      val filterCategoryThird = sourceDf.where(
        s"content = 'filter_category_third' and content_type in ('$contentType','all')"
      ).select("value").withColumnRenamed("value", "filter_category_third").withColumn("fake_id", lit(1))

      sortTypeDf.join(filterCategoryFirst, List("fake_id"), "leftouter")
        .join(filterCategorySecond, List("fake_id"), "leftouter")
        .join(filterCategoryThird, List("fake_id"), "leftouter")
        .withColumn("content_type", lit(contentType))
    })

    var result: DataFrame = null
    dfList.foreach(df => {
      if (result == null) {
        result = df
      } else {
        result = result.unionAll(df)
      }
    })

    result.withColumn("retrieval_key",
      expr("concat(sort_type, '-', filter_category_first, '-', filter_category_second, '-', filter_category_third)"))

  }
}
