package cn.whaley.datawarehouse.dimension.kidsedu

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType
import cn.whaley.datawarehouse.global.SourceType.jdbc
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame

/**
  *喵学堂页面维度表(大首页、详情页)
  * Created by wangning on 2019/2/25 16:55
  */
object LayoutPage extends DimensionBase{

  columns.skName="layout_page_sk"
  columns.primaryKeys = List("page_code", "area_code", "sub_area_code","version")
  columns.trackingColumns = List()
  columns.allColumns = List(
    "page_code",
    "page_name",
    "area_code",
    "area_name",
    "sub_area_code",
    "sub_area_name",
    "version"
  )

  readSourceType = jdbc

  dimensionName = "dim_kidsedu_layout_page"

  fullUpdate = true

  var sourceDbFromPageModule: Map[String, String] = _
  var sourceDbFromPageManage: Map[String, String] = _
  var sourceDbFromModuleManage: Map[String, String] = _
  var sourceDbFromModulePosition: Map[String, String] = _
  var sourceDbFromlayoutPosition: Map[String, String] = _

  sourceDbFromPageModule= MysqlDB.kidseduCms("mtv_page_module_universal", "id", 1, 5000, 50)
  sourceDbFromPageManage= MysqlDB.kidseduCms("mtv_pagemanage_universal", "id", 1, 5000, 50)
  sourceDbFromModuleManage= MysqlDB.kidseduCms("mtv_modulemanage", "id", 1, 5000, 50)
  sourceDbFromModulePosition= MysqlDB.kidseduCms("mtv_module_position", "id", 1, 5000, 50)
  sourceDbFromlayoutPosition= MysqlDB.kidseduCms("mtv_layoutPosition", "id", 1, 5000, 50)

  /**
    * 自定义的源数据读取方法
    * 默认是从jdbc中读取，如需自定义，可以在子类中重载实现
    *
    * @return
    */
  override def readSource(readSourceType: SourceType.Value): DataFrame = {

    sqlContext.read.format("jdbc").options(sourceDbFromPageModule).load.createOrReplaceTempView("mtv_page_module_universal")
    sqlContext.read.format("jdbc").options(sourceDbFromPageManage).load.createOrReplaceTempView("mtv_pagemanage_universal")
    sqlContext.read.format("jdbc").options(sourceDbFromModuleManage).load.createOrReplaceTempView("mtv_modulemanage")
    sqlContext.read.format("jdbc").options(sourceDbFromModulePosition).load.createOrReplaceTempView("mtv_module_position")
    sqlContext.read.format("jdbc").options(sourceDbFromlayoutPosition).load.createOrReplaceTempView("mtv_layoutPosition")

    /**
      * 每个表映射到维度表的字段
      * mtv_page_module_universal ==>page_code
      * mtv_pagemanage_universal ==>title==>page_name
      * mtv_page_module_universal==>module_code==>area_code
      * mtv_modulemanage==>title==>area_name
      * mtv_module_position==>position_code==>sub_area_code
      * mtv_layoutPosition==>title==>sub_area_name
      * mtv_pagemanage_universal==>version_number==>version
      */
    //页面各个维度逻辑处理
    sqlContext.sql(
      """
        |select
        |	distinct b.page_code,a.title as page_name,b.module_code as area_code,d.title as area_name,
        |	c.position_code as sub_area_code,substring_index(e.title,"-",-1) as sub_area_name,a.version_number as version
        |from
        |	mtv_pagemanage_universal a
        |join
        |	mtv_page_module_universal b
        |on
        |	a.id = b.page_id
        |join
        |	mtv_module_position c
        |on
        |	b.module_code = c.module_code
        |join
        |	mtv_modulemanage d
        |on
        |	b.module_code = d.`code`
        |join
        |	mtv_layoutPosition e
        |on
        |	c.position_code = e.`code`
        |where
        |	a.`status` = 1 and b.`status` = 1 and c.`status` = 1
        |	and d.`status` = 1 and e.`status` = 1
      """.stripMargin)
  }
}
