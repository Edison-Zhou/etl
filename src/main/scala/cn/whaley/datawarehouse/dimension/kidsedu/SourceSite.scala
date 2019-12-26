package cn.whaley.datawarehouse.dimension.kidsedu

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType.jdbc
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame

/**
  * 喵学堂站点树维度表
  * Created by wangning on 2019/2/25 11:17
  */
object SourceSite extends DimensionBase {

  columns.skName = "source_site_sk"
  columns.primaryKeys = "source_site_id" :: Nil
  columns.trackingColumns = List()
  columns.allColumns = List(
    "source_site_id",
    "main_category_name",
    "main_category_code",
    "second_category_name",
    "second_category_code"
  )

  readSourceType = jdbc

  sourceDb = MysqlDB.kidseduCms("mtv_program_site", "id", 10, 500, 5)

  dimensionName = "dim_kidsedu_source_site"

  fullUpdate = true

  override def filterSource(sourceDf: DataFrame): DataFrame = {

    //filter
    sourceDf.filter("status = 1").filter("id != 12309").createOrReplaceTempView("mtv_program_site")

    //cache
    sqlContext.cacheTable("mtv_program_site")

    //处理站点树逻辑
    val sourceSiteDf = sqlContext.sql(
      """
        |SELECT
        |	a.id as source_site_id,
        |	b.name as main_category_name,
        |	b.code as main_category_code,
        |	a.name AS second_category_name,
        |	a.code AS second_category_code
        |FROM
        |	`mtv_program_site` a
        |INNER JOIN
        |	mtv_program_site b
        |on
        |	b.id=a.`parentId`
      """.stripMargin)
    sourceSiteDf.orderBy("source_site_id")
  }
}
