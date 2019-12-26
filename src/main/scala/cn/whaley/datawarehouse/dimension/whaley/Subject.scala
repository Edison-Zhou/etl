package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame


/**
  * Created by czw on 3/13/17.
  *
  * 微鲸端节目专题维度表
  */
object Subject extends DimensionBase {

  dimensionName = "dim_whaley_subject"

  columns.skName = "subject_sk"

  columns.primaryKeys = List("subject_code")

  columns.trackingColumns = List()

  columns.allColumns = List(

    "subject_code",
    "subject_name",
    "subject_title",
    "subject_content_type",
    "subject_content_type_name",
    "subject_create_time",
    "subject_publish_time",
    "column_type_id"

  )

  readSourceType = jdbc

  sourceDb = MysqlDB.whaleyCms("mtv_subject", "ID", 1, 4000, 5)


  /**
    * 处理原数据的自定义的方法
    * 默认可以通过配置实现，如果需要自定义处理逻辑，可以再在子类中重载实现
    *
    * @param sourceDf
    * @return
    */
  override def filterSource(sourceDf: DataFrame): DataFrame = {

    val sq = sqlContext
    import sq.implicits._

    val contentTypeDb = MysqlDB.whaleyCms("mtv_content_type", "id", 1, 100, 1)

    val contentTypeDf = sqlContext.read.format("jdbc").options(contentTypeDb).load()
      .select($"code", $"name")

    sourceDf.filter($"code".isNotNull && $"code" != "")
      .as("s")

      .join(contentTypeDf.as("c"), $"s.content_type" === $"c.code", "left_outer")
      .select(
        $"s.code".as(columns.primaryKeys(0)),
        $"s.name".as(columns.allColumns(1)),
        $"s.title".as(columns.allColumns(2)),
        $"c.code".as(columns.allColumns(3)),
        $"c.name".as(columns.allColumns(4)),
        $"s.create_time".cast("timestamp").as(columns.allColumns(5)),
        $"s.publish_time".cast("timestamp").as(columns.allColumns(6)),
        $"s.column_type_id".as(columns.allColumns(7))
      )
  }
}
