package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.JavaConversions._


/**
  * Created by Tony
  * 搜索维度表
  */
object Search extends DimensionBase {

  dimensionName = "dim_medusa_search"

  columns.skName = "search_sk"

  columns.primaryKeys = List("search_from", "search_tab", "search_area_code", "search_from_hot_word", "search_result_index")

  columns.trackingColumns = List()

  columns.allColumns = List(
    "search_from",
    "search_tab",
    "search_area_code",
    "search_from_hot_word",
    "search_result_index")


  readSourceType = jdbc

  sourceDb = MysqlDB.dwDimensionDb("moretv_search")


  override def filterSource(sourceDf: DataFrame): DataFrame = {
    val searchFromDf = sourceDf.where("content = 'search_from'").select("value")
      .withColumnRenamed("value", "search_from").withColumn("fake_id", lit(1))
    val searchTabDf = sourceDf.where("content = 'search_tab'").select("value")
      .withColumnRenamed("value", "search_tab").withColumn("fake_id", lit(1))
    val searchAreaCodeDf = sourceDf.where("content = 'search_area_code'").select("value")
      .withColumnRenamed("value", "search_area_code").withColumn("fake_id", lit(1))

    val searchFromHotWordDf = sqlContext.createDataFrame(
      (0 to 1).map(s => Row.fromSeq(List(s))),
      StructType(Array(StructField("search_from_hot_word", IntegerType)))
    ).withColumn("fake_id", lit(1))

    //搜索结果索引列，其中-1表示未知,-2表示超过100
    val searchResultIndexDf = sqlContext.createDataFrame(
      (-2 to 100).map(s => Row.fromSeq(List(s))),
      StructType(Array(StructField("search_result_index", IntegerType)))
    ).withColumn("fake_id", lit(1))

    searchFromDf.join(searchTabDf, List("fake_id"), "leftouter")
      .join(searchAreaCodeDf, List("fake_id"), "leftouter")
      .join(searchFromHotWordDf, List("fake_id"), "leftouter")
      .join(searchResultIndexDf, List("fake_id"), "leftouter")

  }
}
