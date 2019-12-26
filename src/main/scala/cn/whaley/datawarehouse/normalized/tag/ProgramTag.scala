package cn.whaley.datawarehouse.normalized.tag

import cn.whaley.datawarehouse.normalized.NormalizedEtlBase
import cn.whaley.datawarehouse.util.{DataExtractUtils, MysqlDB, Params}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by Tony on 17/4/19.
  */
object ProgramTag extends NormalizedEtlBase{

  tableName = "program_tag"

  override def extract(params: Params): DataFrame = {
    val sourceDb = MysqlDB.programTag("tag", "id", 1, 500000, 100)
    DataExtractUtils.readFromJdbc(sqlContext,sourceDb).where("status = 1")
  }

  //此工程中,凡涉及transform方法中的参数df,均是由上一步的extract方法产出的
  //transform方法产出了标签id, 标签名, 标签所属二级分类id, 二级分类类型名, 一级标签id, 一级标签名, 来源是否媒资,
  // 来源是否豆瓣, 是否黑名单, 频道类型:1.hot, 2.movie, 3.tv
  override def transform(params: Params, df: DataFrame): DataFrame = {
    val sourceDb = MysqlDB.programTag("tag_type", "id", 1, 1000, 10)
    val tagTypeDf = DataExtractUtils.readFromJdbc(sqlContext,sourceDb).where("status = 1")
    df.as("a").join(
      tagTypeDf.as("b"), expr("a.tag_type_id = b.id"), "leftouter"
    ).join(
      tagTypeDf.as("c"), expr("b.parent_id = c.id"), "leftouter"
    ).selectExpr(
      "a.id",
      "a.tag_name",
      "b.id as type_id",
      "b.type_name",
      "c.id as parent_type_id",
      "c.type_name as parent_type_name",
      "a.is_media_source",
      "a.is_douban",
      "a.is_blacklist",
      "a.tag_type")
  }

  override def load(params: Params, df: DataFrame): Unit = {
    save(params, df)
  }
}
