package cn.whaley.datawarehouse.dimension.whaley

import java.text.SimpleDateFormat
import java.util.Date

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global
import cn.whaley.datawarehouse.util.{DataExtractUtils, MysqlDB}
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.DataFrame

/**
  * Created by czw on 17/3/10.
  *
  * 微鲸节目维度表
  */
object Program extends DimensionBase {

  columns.skName = "program_sk"
  columns.primaryKeys = List("sid")
  columns.trackingColumns = List()
  columns.allColumns = List("sid",
    "program_id",
    "title",
    "content_type",
    "content_type_name",
    "duration",
    "video_type",
    "episode_index",
    "area",
    "year",
    "video_length_type",
    "create_time",
    "publish_time",
    "douban_id",
    "source",
    "language_code",
    "supply_type",
    "tags",
    "product_code",
    "product_name"
  )


  //  sourceDb = MysqlDB.whaleyCms("mtv_basecontent", "id", 1, 2010000000, 500)

  dimensionName = "dim_whaley_program"

  sourceTimeCol = "publish_time"

  fullUpdate = true

  override def readSource(readSourceType: global.SourceType.Value): DataFrame = {
    val date = new SimpleDateFormat("yyyyMMdd").format(DateUtils.addDays(new Date(), -1))
    DataExtractUtils.readFromOds(sqlContext, "ods_view.db_snapshot_mysql_whaley_mtv_basecontent", date, "00")
  }

  override def filterSource(sourceDf: DataFrame): DataFrame = {

    sqlContext.udf.register("myReplace",myReplace _)

    sourceDf.registerTempTable("mtv_basecontent")

    sqlContext.sql("select sid, id as program_id, myReplace(display_name) as title, content_type,  duration, " +
      "video_type, episode as episode_index, area, year, videoLengthType as video_length_type, " +
      "create_time, publish_time, douban_id, source, " +
      "language_code, supply_type, tags," +
      "case when trim(product_code) = '' then null else product_code end as product_code, "+
      "case when trim(product_name) = '' then null when product_name = '基础包' then '白金会员' else product_name end as product_name "+
      " from mtv_basecontent " +
      " where sid is not null and sid <> '' and display_name is not null ")
      .dropDuplicates(List("sid"))
      .registerTempTable("program_table")

    val contentTypeDb = MysqlDB.whaleyCms("mtv_content_type", "id", 1, 100, 1)

    sqlContext.read.format("jdbc").options(contentTypeDb).load().registerTempTable("content_type")

    sqlContext.sql("SELECT a.*, b.name as content_type_name " +
      " from program_table a left join content_type b on a.content_type = b.code " +
      " where a.sid is not null and a.sid <> ''")
  }


  def myReplace(s:String): String ={
    var t = s
    t = t.replace("'", "")
    t = t.replace("\t", " ")
    t = t.replace("\r\n", "-")
    t = t.replace("\r", "-")
    t = t.replace("\n", "-")
    t
  }
}
