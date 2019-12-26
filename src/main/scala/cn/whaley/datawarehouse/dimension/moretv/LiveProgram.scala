package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame

/**
  * Created by witnes on 3/14/17.
  * update by wu.jiulin on 4/19/17.
  * 增加五个字段:live_program_compere(直播播主),live_type(一级直播类型),live_type_name(一级直播类型名称),
  * live_type_2(二级直播类型),live_type_name_2(二级直播类型名称)
  * 直播节目维度表
  */
object LiveProgram extends DimensionBase {

  dimensionName = "dim_medusa_live_program"

  columns.skName = "live_program_sk"

  columns.primaryKeys = List("live_program_sid")

  columns.trackingColumns = List()

  columns.allColumns = List(
    "live_program_sid", "live_program_title", "live_program_source", "live_program_site", "live_program_create_time",
    "live_program_publish_time","live_program_compere","live_type","live_type_name","live_type_2","live_type_name_2"
  )

  debug = false

  readSourceType = jdbc

  sourceDb = MysqlDB.medusaCms("mtv_live_program", "id", 1, 250, 1)

  sourceTimeCol = "publish_time"

  override def filterSource(sourceDf: DataFrame): DataFrame = {

    val sq = sqlContext
    import sq.implicits._

    val liveTypeDb = MysqlDB.dwDimensionDb("moretv_live_type")
    val liveTypeDf = sqlContext.read.format("jdbc").options(liveTypeDb).load()
      .select($"live_type", $"live_type_name", $"live_type_2", $"live_type_name_2")

    val sourceDfFilter = sourceDf.filter($"sid".isNotNull).dropDuplicates("sid" :: Nil)
    sourceDfFilter.as("b").join(liveTypeDf.as("a"), liveTypeDf("live_type_2")===sourceDfFilter("live_type_2"), "left_outer")
      .select(
        $"b.sid".as(columns.primaryKeys(0)),
        $"b.title".as(columns.allColumns(1)),
        $"b.source".as(columns.allColumns(2)),
        $"b.site".as(columns.allColumns(3)),
        $"b.create_time".cast("timestamp").as(columns.allColumns(4)),
        $"b.publish_time".cast("timestamp").as(columns.allColumns(5)),
        $"b.program_compere".as(columns.allColumns(6)),
        $"a.live_type".as(columns.allColumns(7)),
        $"a.live_type_name".as(columns.allColumns(8)),
        $"a.live_type_2".as(columns.allColumns(9)),
        $"a.live_type_name_2".as(columns.allColumns(10))
      )

  }


}
