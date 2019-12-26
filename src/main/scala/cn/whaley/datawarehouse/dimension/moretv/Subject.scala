package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame


/**
  * Created by witnes on 3/13/17.
  *
  * 电视猫_节目专题维度表
  */
object Subject extends DimensionBase {

  dimensionName = "dim_medusa_subject"

  columns.skName = "subject_sk"

  columns.primaryKeys = List("subject_code")

  columns.trackingColumns = List("subject_name")

  columns.allColumns = List(
    "subject_code",
    "subject_name",
    //    "subject_title",
    "subject_content_type",
    "subject_content_type_name",
    "subject_mode",
    "subject_create_time",
    "subject_publish_time"
  )

  readSourceType = jdbc

  sourceDb = MysqlDB.medusaTvService("mtv_subject", "ID", 1, 4000, 5)


  /**
    * 处理原数据的自定义的方法
    * 默认可以通过配置实现，如果需要自定义处理逻辑，可以再在子类中重载实现
    *
    * @param sourceDf
    * @return
    */
  override def filterSource(sourceDf: DataFrame): DataFrame = {

    val sq = sqlContext
    import org.apache.spark.sql.functions._
    import sq.implicits._

    val contentTypeDb = MysqlDB.medusaCms("mtv_content_type", "id", 1, 100, 1)
    val subjectPageDb = MysqlDB.medusaTvService("mtv_subject_page", "ID", 1, 4000, 5)

    val contentTypeDf = sqlContext.read.format("jdbc").options(contentTypeDb).load()
      .select($"code", $"name")
    val subjectPageDf = sqlContext.read.format("jdbc").options(subjectPageDb).load()
      .select($"mode", $"subject_id", $"status")
      .filter("status = 1")
      .distinct()
    subjectPageDf.createOrReplaceTempView("mtv_subject_page")
    sqlContext.sql(
      """
        |select case mode when '1' then '排行榜' when '2' then '短视频' when '3' then '双列' when '4' then '时间线'
        | when '5' then '少儿'  when '6' then '应用'  when '7' then '时间轴'  when '8' then '预告片'  when '9' then 'pk专题'
        | when '10' then '体育' when '11' then '小视频' else '需要更新维度表' end as subject_mode ,subject_id
        | from mtv_subject_page
      """.stripMargin).createOrReplaceTempView("subject_mode") //专题模板信息

    sourceDf.filter($"code".isNotNull && $"code".notEqual("")
      && $"name".isNotNull && $"name".notEqual("")
      && $"code".notEqual("apptest") //apptest对应了两个模板，无法确认是哪个模板，又因为是测试专题，所以删除。
      && $"status".notEqual(-1))
      .withColumn("codev", regexp_extract($"code", "[a-z]*", 0)).as("s")
      .join(contentTypeDf.as("c"), $"s.codev" === $"c.code", "left_outer")
      .select(
        $"s.id".as("subject_id"),
        $"s.code".as("subject_code"),
        $"s.name".as("subject_name"),
        $"s.title".as("subject_title"),
        $"c.code".as("subject_content_type"),
        $"c.name".as("subject_content_type_name"),
        $"s.create_time".cast("timestamp").as("subject_create_time"),
        $"s.publish_time".cast("timestamp").as("subject_publish_time")
      ).createOrReplaceTempView("sourceDfTemp")
    sqlContext.sql(
      """
        |select a.*,b.subject_mode
        |from sourceDfTemp a left join subject_mode b
        |on a.subject_id = b.subject_id
      """.stripMargin)

  }
}
