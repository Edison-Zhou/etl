package cn.whaley.datawarehouse.normalized.tag

import cn.whaley.datawarehouse.normalized.NormalizedEtlBase
import cn.whaley.datawarehouse.util.{DataExtractUtils, MysqlDB, Params}
import org.apache.spark.sql.DataFrame

/**
  * Created by Tony on 17/4/19.
  * Reused by Edison_Zhou on 19/3/9
  */
object UseeProgramTagMapping extends NormalizedEtlBase {

  tableName = "youku_program_tag_mapping"

  override def extract(params: Params): DataFrame = {
    val sourceDb = MysqlDB.programTag("tag_youku_program_mapping", "id", 1, 30000000, 2000)
    val programTagDf = DataExtractUtils.readFromJdbc(sqlContext, sourceDb).where("status = 1")

    val sourceDb2 = MysqlDB.programTag("tag_mapping", "id", 1, 2000, 10)
    val tagMappingDf = DataExtractUtils.readFromJdbc(sqlContext, sourceDb2).where("status = 1")

    val sourceDb3 = MysqlDB.programTag("tag_youku_program", "id", 1, 6000000, 200)
    val programDf = DataExtractUtils.readFromJdbc(sqlContext, sourceDb3).where("status = 1")

    val sourceDb4 = MysqlDB.programTag("tag", "id", 1, 2000000, 100)
    val tagDf = DataExtractUtils.readFromJdbc(sqlContext, sourceDb4).where("status = 1")

    val df = programTagDf
      .join(programDf, List("sid"), "leftouter")
      .selectExpr("sid", "title", "tag_id", "tag_level_value", "cast(tag_source as int) tag_source", "type", "content_type").as("a")
      .join(tagMappingDf.as("b"), programTagDf("tag_id") === tagMappingDf("tag_id"), "leftouter")
      .selectExpr(
      "sid",
      "title",
      "case when b.mapping_tag_id is null then a.tag_id else b.mapping_tag_id end as tag_id",
      "tag_level_value",
      "tag_source",
      "type",
      "content_type"
    )
    df.as("p").join(
      tagDf.as("t"), df("tag_id") === tagDf("id"), "leftouter"
    ).selectExpr(
      "sid as program_sid",
      "title as program_title",
      "tag_id",
      "t.tag_name",
      "tag_level_value",
      "tag_source",     //节目标签来源: 1.媒资导入 2.人工标记 3.智能算法
      "type as program_type",   //1为正片 2是片花 3微电影
      "content_type"
    )
  }


  override def transform(params: Params, df: DataFrame): DataFrame = {
    df
  }

  override def load(params: Params, df: DataFrame): Unit = {
    save(params, df)
  }
}
