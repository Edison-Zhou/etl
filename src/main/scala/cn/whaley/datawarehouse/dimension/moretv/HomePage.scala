package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

import scala.collection.JavaConversions._

/**
  * Created by Zhu.bingxin on 18/09/19.
  */
object HomePage extends DimensionBase {

  columns.skName = "home_page_sk"
  columns.primaryKeys = List("page_code", "area_code", "sub_area_code", "location_index", "version")
  columns.trackingColumns = List()
  columns.allColumns = List(
    "home_page_id",
    "page_code",
    "page_name",
    "area_code",
    "area_name",
    "sub_area_code",
    "version",
    "location_index")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "home_page_id" -> "id"
  )

  //  sourceDb = MysqlDB.dwDimensionDb("moretv_page_entrance")

  val SourceType = jdbc
  //读取电视猫cms中的表
  var sourceDbFromModulePosition: Map[String, String] = _
  var sourceDbFromDetailPage: Map[String, String] = _
  var sourceDbFromDetailModuleCode: Map[String, String] = _

  sourceDbFromModulePosition = MysqlDB.medusaCms("mtv_module_position", "id", 1, 3000, 20)
  sourceDbFromDetailPage = MysqlDB.medusaCms("mtv_pagemanage_universal", "id", 1, 3000, 20)
  sourceDbFromDetailModuleCode = MysqlDB.medusaCms("mtv_page_module_universal", "id", 1, 3000, 20)
  sourceDbFromDetailModuleName = MysqlDB.medusaCms("mtv_modulemanage", "id", 1, 3000, 20)

  dimensionName = "dim_medusa_home_page"

  fullUpdate = true
  var sourceDbFromDetailModuleName: Map[String, String] = _

  override def readSource(SourceType: Value): Dataset[Row] = {

    //读另外一个表, 并且跟df对齐
    sqlContext.read.format("jdbc").options(sourceDbFromModulePosition).load().createTempView("mtv_module_position")
    sqlContext.read.format("jdbc").options(sourceDbFromDetailPage).load().createTempView("mtv_pagemanage_universal")
    sqlContext.read.format("jdbc").options(sourceDbFromDetailModuleCode).load().createTempView("mtv_page_module_universal")
    sqlContext.read.format("jdbc").options(sourceDbFromDetailModuleName).load().createTempView("mtv_modulemanage")


    //取出所需列生成新的df
    sqlContext.sql(
      s"""
         |select a.id,b.page_code,a.`name` as page_name,b.module_code as area_code,d.name as area_name
         |,c.position_code as sub_area_code,a.version_number as version
         |from mtv_pagemanage_universal a join mtv_page_module_universal b
         |on a.id = b.page_id join mtv_module_position c
         |on b.module_code = c.module_code join mtv_modulemanage d
         |on b.module_code = d.`code`
         |where a.`status` = 1 and b.`status` = 1 and c.`status` = 1 and d.`status` = 1""".stripMargin)
      .withColumn("fake_id", lit(1))
      .createTempView("page")

    //位置序号白名单
    val locationIndexList = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 201, 202, 203, 204, 205)

    sqlContext.createDataFrame(
      locationIndexList.map(s => Row.fromSeq(List(s.toString))),
      StructType(Array(StructField("location_index", StringType)))
    ).withColumn("fake_id", lit(1)).createTempView("index")

    val result = sqlContext.sql(
      """
        |select a.*,b.location_index
        |from page a join index b
        |on a.fake_id = b.fake_id
      """.stripMargin)
    result
  }
}
