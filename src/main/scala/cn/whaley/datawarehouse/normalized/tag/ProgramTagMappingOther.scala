package cn.whaley.datawarehouse.normalized.tag

import java.util.Date

import cn.whaley.datawarehouse.global.Globals
import cn.whaley.datawarehouse.normalized.NormalizedEtlBase
import cn.whaley.datawarehouse.util.{DataExtractUtils, DateFormatUtils, MysqlDB, Params}
import org.apache.spark.sql.DataFrame


/**
  * Created by Tony on 17/4/19.
  */
object ProgramTagMappingOther extends NormalizedEtlBase {
  val sqlC = sqlContext

  import sqlC.implicits._

  tableName = "program_tag_mapping_other"

  override def extract(params: Params): DataFrame = {

//    val sourceDb = MysqlDB.medusaCms("mtv_basecontent", "id", 1, 2010000000, 2000)
//    var programDf = sqlContext.read.format("jdbc").options(sourceDb).load()

    val date = DateFormatUtils.readFormat.format(new Date())
    var programDf = DataExtractUtils.readFromOds(sqlContext, "ods_view.db_snapshot_mysql_medusa_mtv_basecontent", date, "00")
    if(programDf.count() == 0) {
      throw new RuntimeException(date + "的数据快照db_snapshot_mysql_medusa_mtv_basecontent为空")
    }
    programDf = programDf.dropDuplicates(List("sid"))

    programDf.persist()

    val zongyiDf = programDf.where("content_type = 'zongyi'")
    var result = getTagDf(zongyiDf, "toastmaster", "主持人")
    result = getTagDf(zongyiDf, "guest", "嘉宾").unionAll(result)
    result = getTagDf(zongyiDf, "tags", "关联标签").unionAll(result)
    result = getTagDf(zongyiDf, "area", "产地").unionAll(result)
    result = getTagDf(zongyiDf, "station", "出品方").unionAll(result)
    result = getTagDfInt(zongyiDf, "year", "年代").unionAll(result)

    val jiluDf = programDf.where("content_type = 'jilu'")
    result = getTagDf(jiluDf, "tags", "关联标签").unionAll(result)
    result = getTagDf(jiluDf, "area", "产地").unionAll(result)
    result = getTagDf(jiluDf, "station", "出品方").unionAll(result)
    result = getTagDfInt(jiluDf, "year", "年代").unionAll(result)

    val kidsAndComicDf = programDf.where("content_type = 'kids' or content_type = 'comic'")
    result = getTagDf(kidsAndComicDf, "area", "产地").unionAll(result)
    result = getTagDfInt(kidsAndComicDf, "year", "年代").unionAll(result)
    result = getTagDf(kidsAndComicDf, "tags", "关联标签").unionAll(result)
    result = getTagDf(kidsAndComicDf, "director", "导演").unionAll(result)

    val liveDf = sqlContext.read.parquet(Globals.DIMENSION_HDFS_BASE_PATH + "/dim_medusa_live_program")
    result = getLiveTagDf(liveDf, "live_type_name", "分类").unionAll(result)
    result = getLiveTagDf(liveDf, "live_type_name_2", "分类").unionAll(result)
    result = getLiveTagDf(liveDf, "live_program_compere", "主播").unionAll(result)

    result

  }

  override def transform(params: Params, df: DataFrame): DataFrame = {
    df
  }

  /**
    * 字段必须是字符串类型
    */
  private def getTagDf(df: DataFrame, columnName: String, tagTypeName: String): DataFrame = {
    df.where(s"$columnName is not null and trim($columnName) <> ''").explode(
      columnName, s"each_$columnName"
    ) { values: String => values.trim.split("\\|") }
      .selectExpr(
        "sid as program_sid",
        "display_name as program_title",
        "content_type as content_type",
        s"trim(each_$columnName) as tag_name",
        s"'$tagTypeName' as tag_type")
  }

  /**
    * 字段必须是数字类型
    */
  private def getTagDfInt(df: DataFrame, columnName: String, tagTypeName: String): DataFrame = {
    df.where(s"$columnName is not null and $columnName > 0").selectExpr(
      "sid as program_sid",
      "display_name as program_title",
      "content_type as content_type",
      s"cast($columnName as string) as tag_name",
      s"'$tagTypeName' as tag_type")
  }

  /**
    * 字段必须是字符串类型
    */
  private def getLiveTagDf(df: DataFrame, columnName: String, tagTypeName: String): DataFrame = {
    df.where(s"$columnName is not null and trim($columnName) <> ''").selectExpr(
      "live_program_sid as program_sid",
      "live_program_title as program_title",
      "'live' as content_type",
      s"trim($columnName) as tag_name",
      s"'$tagTypeName' as tag_type")
  }

  override def load(params: Params, df: DataFrame): Unit = {
    save(params, df)
  }

}
