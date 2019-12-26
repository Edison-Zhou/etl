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
object Search4x extends DimensionBase {

  dimensionName = "dim_medusa4x_search"

  columns.skName = "search_sk"

  columns.primaryKeys = List("search_from", "search_tab", "search_area_code")

  columns.allColumns = List(
    "search_from",
    "search_area_code",
    "search_tab")

  readSourceType = jdbc

  sourceDb = MysqlDB.dwDimensionDb("moretv_search4x")

}
