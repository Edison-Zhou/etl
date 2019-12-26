package cn.whaley.datawarehouse.dimension.utvmore

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._

/**
  * Created by Zhu.bingxin on 2019/05/07.
  *
  * 布局页面（有模块、行code、位置code的页面，目前有大首页、频道首页、专区首页、详情页）
  * 位置code目前只有大首页的我的和分类区域有
  */
object LayoutPage extends DimensionBase {

  columns.skName = "layout_page_sk"
  columns.primaryKeys = List("page_code", "area_code", "sub_area_code", "location_code", "location_index", "version")
  columns.trackingColumns = List()
  columns.allColumns = List(
    "page_code",
    "page_name",
    "area_code",
    "area_name",
    "sub_area_code",
    "sub_area_name",
    "location_code",
    "version",
    "location_index")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  //  sourceColumnMap = Map(
  //    "layout_page_id" -> "id"
  //  )


  val SourceType = jdbc
  //读取电视猫cms中的表
  var sourceDbFromModulePosition: Map[String, String] = _
  var sourceDbFromDetailPage: Map[String, String] = _
  var sourceDbFromDetailModuleCode: Map[String, String] = _
  var sourceDbFromDetailModuleName: Map[String, String] = _
  var sourceDbFromDetailLayoutPosition: Map[String, String] = _
  var sourceDbFromDetailLayoutPositionItem: Map[String, String] = _


  sourceDbFromModulePosition = MysqlDB.utvmoreGeminiTvservice("mtv_module_position", "id", 1, 3000, 20)
  sourceDbFromDetailPage = MysqlDB.utvmoreGeminiTvservice("mtv_pagemanage_universal", "id", 1, 3000, 20)
  sourceDbFromDetailModuleCode = MysqlDB.utvmoreGeminiTvservice("mtv_page_module_universal", "id", 1, 3000, 20)
  sourceDbFromDetailModuleName = MysqlDB.utvmoreGeminiTvservice("mtv_modulemanage", "id", 1, 3000, 20)
  sourceDbFromDetailLayoutPosition = MysqlDB.utvmoreGeminiTvservice("mtv_layoutPosition", "id", 1, 3000, 20)
  sourceDbFromDetailLayoutPositionItem = MysqlDB.utvmoreGeminiTvservice("mtv_layoutPositionItem", "id", 1, 3000, 20)

  dimensionName = "dim_utvmore_layout_page"

  fullUpdate = true


  override def readSource(SourceType: Value): Dataset[Row] = {

    //读另外一个表, 并且跟df对齐
    sqlContext.read.format("jdbc").options(sourceDbFromModulePosition).load().createTempView("mtv_module_position")
    sqlContext.read.format("jdbc").options(sourceDbFromDetailPage).load().createTempView("mtv_pagemanage_universal")
    sqlContext.read.format("jdbc").options(sourceDbFromDetailModuleCode).load().createTempView("mtv_page_module_universal")
    sqlContext.read.format("jdbc").options(sourceDbFromDetailModuleName).load().createTempView("mtv_modulemanage")
    sqlContext.read.format("jdbc").options(sourceDbFromDetailLayoutPosition).load().createTempView("mtv_layoutPosition")
    sqlContext.read.format("jdbc").options(sourceDbFromDetailLayoutPositionItem).load().createTempView("mtv_layoutPositionItem")


    /**
      * 取出所需列生成新的df
      * union了两部分的数据
      * 1）针对大首页的"我的"和"分类"区域精确到location_code,需要注意的是这里的module_code和电视猫的不一样，"我的"为ysm_my,
      * "分类"为ysm_classification
      * 2）其他页面精确到sub_area_code，不能直接使用mtv_layoutPositionItem关联得到，该表中的有效记录没有其他页面的位置信息
      * 3）mtv_layoutPositionItem中的link_value的值对应的是location_code
      * 4）除大首页外的location_code为空字符串
      * 5）mtv_layoutPosition中的title字段截取末尾下划线和数字后作为sub_area_name
      */
    sqlContext.sql(
      """
        |select distinct b.page_code,a.title as page_name,b.module_code as area_code,d.title as area_name
        |,c.position_code as sub_area_code,split(e.title,'_[0-9]+$')[0] as sub_area_name,f.link_value as location_code
        |,a.version_number as version
        |from mtv_pagemanage_universal a join mtv_page_module_universal b
        |on a.id = b.page_id join mtv_module_position c
        |on b.module_code = c.module_code join mtv_modulemanage d
        |on b.module_code = d.`code` join mtv_layoutPosition e
        |on c.position_code = e.`code` join mtv_layoutPositionItem f
        |on c.position_code = f.position_code
        |where a.`status` = 1 and b.`status` = 1 and c.`status` = 1 and d.`status` = 1 and e.`status` = 1 and f.`status` = 1
        |and (b.module_code = 'ysm_my' or b.module_code = 'ysm_classification')
        |union
        |select distinct b.page_code,a.title as page_name,b.module_code as area_code,d.title as area_name
        |,c.position_code as sub_area_code,split(e.title,'_[0-9]+$')[0] as sub_area_name,'' as location_code
        |,a.version_number as version
        |from mtv_pagemanage_universal a join mtv_page_module_universal b
        |on a.id = b.page_id join mtv_module_position c
        |on b.module_code = c.module_code join mtv_modulemanage d
        |on b.module_code = d.`code` join mtv_layoutPosition e
        |on c.position_code = e.`code`
        |where a.`status` = 1 and b.`status` = 1 and c.`status` = 1 and d.`status` = 1 and e.`status` = 1
        |and (b.module_code != 'ysm_my' and b.module_code != 'ysm_classification')
      """.stripMargin).withColumn("fake_id", lit(1)).createTempView("page")

    //    println("########  Begin print location_code  ########")
    //    sqlContext.sql(
    //      """
    //        |select distinct location_code from page
    //      """.stripMargin).show(20, false)

    //位置序号白名单
    val locationIndexList = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 101, 102, 103,
      104, 105, 201, 202, 203, 204, 205, 301, 302, 303, 304, 305, 401, 402, 403, 404, 405, 501, 502, 503, 504, 505)

    sqlContext.createDataFrame(
      locationIndexList.map(s => Row.fromSeq(List(s.toString))),
      StructType(Array(StructField("location_index", StringType)))
    ).withColumn("fake_id", lit(1)).createTempView("index")


    val originalDf = sqlContext.sql(
      """
        |select a.*,b.location_index
        |from page a join index b
        |on a.fake_id = b.fake_id
      """.stripMargin)

    //往layout_page维度表里插入一条开屏页数据
    val schema = StructType(List(
      StructField("page_code", StringType),
      StructField("page_name", StringType),
      StructField("area_code", StringType),
      StructField("area_name", StringType),
      StructField("sub_area_code", StringType),
      StructField("sub_area_name", StringType),
      StructField("location_code", StringType),
      StructField("version", StringType),
      StructField("fake_id", StringType),
      StructField("location_index", StringType)
    ))
    val rdd = spark.sparkContext.parallelize(Seq(Row("open_screen", "开屏页", "", null, "", null, "", "1", null, "")))
    val open_screenDF = spark.createDataFrame(rdd, schema)

    val result = originalDf.union(open_screenDF)

    result
  }


}
