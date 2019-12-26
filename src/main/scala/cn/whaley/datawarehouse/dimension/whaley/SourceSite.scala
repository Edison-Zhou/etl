package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by 郭浩 on 17/5/08.
  * 站点树
  */
object SourceSite extends DimensionBase {
  columns.skName = "source_site_sk"
  columns.primaryKeys = List("source_site_id")
  columns.trackingColumns = List()
  columns.allColumns = List(
    "source_site_id",
    "site_content_type",
    "second_category",
    "second_category_code",
    "third_category",
    "third_category_code",
    "fourth_category",
    "fourth_category_code",
    "last_second_code",
    "last_second_name",
    "last_first_code",
    "last_first_name")

  readSourceType = jdbc

  sourceDb = MysqlDB.whaleyCms("mtv_program_site", "id", 1, 500, 5)

  dimensionName = "dim_whaley_source_site"

  fullUpdate = true

  override def filterSource(sourceDf: DataFrame): DataFrame = {

    //补充站点树
    val whaleySourceSite = MysqlDB.dwDimensionDb("whaley_source_site")
    val df1 = sqlContext.read.format("jdbc").options(whaleySourceSite).load()

    sourceDf.filter("status = 1").filter("trim(name) != '站点树' ")
      .withColumn("contentType", expr("case when id = 1101 then 'sports' else contentType end"))
      .withColumn("contentType", expr("case when id = 1361 then 'sports' else contentType end"))
      .registerTempTable("mtv_program_site")

    //cms 表中获取
    //最大包含四级目录，从包含4级目录的站点树开始
    val df4 = sqlContext.sql("SELECT a.id, " +
      "concat_ws('-',a.code,b.code,c.code,d.contentType) source_site_id, " +
      "d.contentType site_content_type, " +
      //      "d.name AS main_category, d.code AS main_category_code, " +
      "c.name AS second_category, c.code AS second_category_code, " +
      "b.name AS third_category, b.code AS third_category_code," +
      "a.name AS fourth_category, case when (trim(a.templateCode)!= '' and a.contentType='sports') then concat_ws('_',a.code,a.templateCode) else a.code end fourth_category_code, " +
      " b.code AS last_second_code , " +
      " b.name AS last_second_name , " +
      "case when (trim(a.templateCode)!= '' and a.contentType='sports') then concat_ws('_',a.code,a.templateCode) else a.code end last_first_code , " +
      " a.name AS last_first_name  " +
      " FROM mtv_program_site AS a " +
      " INNER JOIN mtv_program_site AS b ON ( a.parentId = b.id)" +
      " INNER JOIN mtv_program_site AS c ON ( b.parentId = c.id)" +
      " INNER JOIN mtv_program_site AS d ON ( c.parentId = d.id and d.status = 1) " +
      " WHERE d.parentId IN (0, 1)")

    val df3 = sqlContext.sql("SELECT a.id," +
      "concat_ws('-',a.code,b.code,c.contentType) source_site_id, " +
      "c.contentType site_content_type," +
      "b.name AS second_category, b.code AS second_category_code, " +
      "a.name AS third_category, a.code AS third_category_code," +
      "null AS fourth_category, null AS fourth_category_code ," +
      " b.code AS last_second_code , " +
      " b.name AS last_second_name , " +
      " a.code AS last_first_code , " +
      " a.name AS last_first_name  " +
      " FROM mtv_program_site AS a " +
      " INNER JOIN mtv_program_site AS b ON ( a.parentId = b.id )" +
      " INNER JOIN mtv_program_site AS c ON ( b.parentId = c.id and c.status = 1)" +
      " WHERE c.parentId IN (0,1)")

    val df2 = sqlContext.sql("SELECT a.id," +
      "concat_ws('-',a.code,b.contentType) as source_site_id," +
      "b.contentType AS site_content_type, " +
      "a.name AS second_category, a.code AS second_category_code, " +
      "null AS third_category, null AS third_category_code, " +
      "null AS fourth_category, null AS fourth_category_code, " +
      " b.contentType AS last_second_code , " +
      " b.contentType AS last_second_name , " +
      " a.code AS last_first_code , " +
      " a.name AS last_first_name  " +
      " FROM mtv_program_site AS a " +
      " INNER JOIN mtv_program_site AS b ON ( a.parentId = b.id and b.status = 1)" +
      " WHERE b.parentId IN (0,1)")

    val arr = Array("source_site_id")
    df4.unionAll(df3).unionAll(df2).unionAll(df1).orderBy(col("id").desc).dropDuplicates(arr)

  }

}
