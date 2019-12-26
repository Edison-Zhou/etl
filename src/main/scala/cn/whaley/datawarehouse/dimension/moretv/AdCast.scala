package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.{Dataset, Row}

/**
  * Created by lian.kai on 2018/11/13.
  */
object AdCast extends DimensionBase {

  columns.skName = "ad_cast_sk"
  columns.primaryKeys = List("ad_putting_id")
  columns.trackingColumns = List()
  columns.allColumns = List(
    "ad_putting_id",
    "ad_project_id",
    "ad_place_id",
    "ad_putting_name",
    "template_id",
    "link_type",
    "link_value",
    "link_title")

  readSourceType = jdbc

  val SourceType = jdbc

  val sourceDbFromAdTvmorePutting: Map[String, String] = MysqlDB.ams("ad_tvmore_putting")
  val sourceDbFromAdTvmorePuttingProgramMetadata: Map[String, String] = MysqlDB.ams("ad_tvmore_putting_program_metadata")

  dimensionName = "dim_medusa_ad_cast"

  fullUpdate = true

  override def readSource(SourceType: Value): Dataset[Row] = {

    //读另外一个表, 并且跟df对齐
    sqlContext.read.format("jdbc").options(sourceDbFromAdTvmorePutting).load().createTempView("ad_tvmore_putting")
    sqlContext.read.format("jdbc").options(sourceDbFromAdTvmorePuttingProgramMetadata).load().createTempView("ad_tvmore_putting_program_metadata")


    //取出所需列生成新的df
    sqlContext.sql(
      s"""
         |select a.id as ad_putting_id, a.adProjectId as ad_project_id,
         | a.adPlaceId as ad_place_id, a.adPuttingName as ad_putting_name,
         | a.templateId as template_id, b.link_type, b.link_value, b.link_title
         | from ad_tvmore_putting a left join ad_tvmore_putting_program_metadata b
         | on a.id = b.id """.stripMargin)

  }
}
