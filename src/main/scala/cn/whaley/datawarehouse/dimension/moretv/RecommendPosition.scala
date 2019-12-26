package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
  * Created by Tony
  * 智能推荐位维度表
  */
object RecommendPosition extends DimensionBase {

  dimensionName = "dim_medusa_recommend_position"

  columns.skName = "recommend_position_sk"

  columns.primaryKeys = List("recommend_position",
    "recommend_position_type",
    "recommend_slot_index",
    "recommend_algorithm")

  columns.trackingColumns = List()

  columns.allColumns = List(
    "recommend_position",
    "recommend_position_name",
    "recommend_position_type",
    "biz",
    "biz_name",
    "recommend_slot_index",
    "recommend_method",
    "recommend_algorithm")


  readSourceType = jdbc

  sourceDb = MysqlDB.dwDimensionDb("moretv_recommend_position")


  override def filterSource(sourceDf: DataFrame): DataFrame = {

    val newSourceDf = sourceDf.where("status = 1")
      .withColumnRenamed("position", "recommend_position")
      .withColumnRenamed("position_name", "recommend_position_name")
      .withColumnRenamed("position_type", "recommend_position_type")

    val getRecommendMethodUdf = udf(getRecommendMethod: (String, Int) => Int)

    newSourceDf.explode("max_index", "recommend_slot_index") {
      maxIndex: Int => {
        (-1 to maxIndex).toList
      }
    }.withColumn(
      "recommend_method", getRecommendMethodUdf(col("ai_index"), col("recommend_slot_index"))
    ).explode("algorithms", "recommend_algorithm") {
      (algorithms: String) => {
        if (algorithms == null || algorithms.trim == "") {
          List("未知")
        } else {
          "未知" :: algorithms.split(",").toList
        }
      }
    }
  }

  private def getRecommendMethod(aiIndex: String, slotIndex: Int): Int = {
    if (aiIndex == "all") {
      1
    } else if (aiIndex == null || aiIndex.trim == "") {
      0
    } else {
      val indices = aiIndex.split(",").map(_.toInt)
      if (indices.contains(slotIndex)) 1 else 0
    }
  }


}
