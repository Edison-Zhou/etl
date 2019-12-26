package cn.whaley.datawarehouse.dimension.whaley

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

  dimensionName = "dim_whaley_search"

  columns.skName = "search_sk"

  columns.primaryKeys = List("search_key")

  columns.trackingColumns = List()

  columns.allColumns = List(
    "search_key",
    "search_from",
    "search_tab",
    "search_from_hot_word",
    "search_from_associational_word",
    "search_result_index")


  readSourceType = jdbc

  sourceDb = MysqlDB.dwDimensionDb("whaley_search")


  override def filterSource(sourceDf: DataFrame): DataFrame = {
    val searchFromDf = sourceDf.where("content = 'search_from'").select("value", "column_index")
      .withColumnRenamed("value", "search_from")
      .withColumnRenamed("column_index", "search_from_column_index")
      .withColumn("fake_id", lit(1))

    val searchTabDf = sourceDf.where("content = 'search_tab'").select("value", "column_index")
      .withColumnRenamed("value", "search_tab")
      .withColumnRenamed("column_index", "search_tab_column_index")
      .withColumn("fake_id", lit(1))

    val searchFromHotWordDf = sqlContext.createDataFrame(
      (0 to 1).map(s => Row.fromSeq(List(s))),
      StructType(Array(StructField("search_from_hot_word", IntegerType)))
    ).withColumn("fake_id", lit(1))

    val searchFromAssociationalWordDf = sqlContext.createDataFrame(
      (0 to 1).map(s => Row.fromSeq(List(s))),
      StructType(Array(StructField("search_from_associational_word", IntegerType)))
    ).withColumn("fake_id", lit(1))

    //搜索结果索引列，其中0表示未知或者超过100
    val searchResultIndexDf = sqlContext.createDataFrame(
      (-2 to 100).map(s => Row.fromSeq(List(s))),
      StructType(Array(StructField("search_result_index", IntegerType)))
    ).withColumn("fake_id", lit(1))

    searchFromDf.join(searchTabDf, List("fake_id"), "leftouter")
      .join(searchFromHotWordDf, List("fake_id"), "leftouter")
      .join(searchFromAssociationalWordDf, List("fake_id"), "leftouter")
      .join(searchResultIndexDf, List("fake_id"), "leftouter")
      .withColumn("search_key",
        expr("concat(search_from_column_index, '_', search_tab_column_index, '_', search_from_hot_word, '_', " +
          " search_from_associational_word, '_', search_result_index)"))
  }
}
