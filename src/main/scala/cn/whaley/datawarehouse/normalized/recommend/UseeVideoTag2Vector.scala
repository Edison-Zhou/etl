package cn.whaley.datawarehouse.normalized.recommend

import cn.whaley.datawarehouse.global.Globals
import cn.whaley.datawarehouse.normalized.NormalizedEtlBase
import cn.whaley.datawarehouse.util.{DataExtractUtils, Params}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.io.File

/**
  * Reused by Edison_Zhou on 2019/3/8.
  */
object UseeVideoTag2Vector extends NormalizedEtlBase {
  tableName = "youkuVideoTagFeature"

  override def extract(params: Params): DataFrame = {
    val tableName = "youku_program_tag_mapping"
    val path: String = Globals.NORMALIZED_TABLE_HDFS_BASE_PATH + File.separator + tableName + File.separator + "current"
    val videoTagDF = DataExtractUtils.readFromParquet(sqlContext, path)
    videoTagDF.selectExpr("program_sid", "tag_id", "tag_level_value").dropDuplicates("program_sid", "tag_id")
  }

  override def transform(params: Params, df: DataFrame): DataFrame = {
    val ss: SparkSession = spark
    import ss.implicits._
    val tagVectorSize = df.agg("tag_id" -> "max").head().getInt(0)
    df.rdd.map(e => (e.getString(0), (e.getInt(1), e.getInt(2))))
      .groupByKey().map(e => {
      val sid = e._1
      val videoTags = e._2
      val tagVectorSequence = new ArrayBuffer[(Int, Double)]()

      videoTags.toArray.foreach(e => {
        val tagId = e._1
        val tag_level = e._2

        if (tagId > 0) {
          val tag_value = tag_level match {
            case 0 => 0.625 //未分类
            case 1 => 1.0 //核心
            case 2 => 0.75 //重要
            case 3 => 0.5 //主要
            case 4 => 0.25 //一般
          }
          tagVectorSequence += ((tagId - 1, tag_value))
        }
      })
      val tagVector = Vectors.sparse(tagVectorSize, tagVectorSequence).toSparse
      (sid, tagVector)
    }).toDF("videoSid", "tagFeatures")
  }

  override def load(params: Params, df: DataFrame): Unit = {
    save(params, df)
  }
}
