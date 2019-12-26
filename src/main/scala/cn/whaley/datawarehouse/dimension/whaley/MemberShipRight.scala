package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame

/**
  * Created by 郭浩 on 17/4/13.
  *
  * 微鲸会员权益
  * 1.过滤sn 以XX,XY,XZ,YX,YY,YZ,ZX开头的
  * 2.只要status为1的
  */
object MemberShipRight extends DimensionBase {
  columns.skName = "membership_right_sk"
  columns.primaryKeys = List("membership_right_id")
  columns.trackingColumns = List()
  columns.allColumns = List("membership_right_id","membership_account",
    "membership_account_nickname","product_sn","product_id","product_name",
    "start_time","effective_time","create_time","update_time")

  readSourceType = jdbc

  sourceDb = MysqlDB.whaleyDolphin("dolphin_club_authority","id",1, 1000000000, 10)

  dimensionName = "dim_whaley_membership_right"

  fullUpdate = true

  override def filterSource(sourceDf: DataFrame): DataFrame = {
    val sq = sqlContext
    import sq.implicits._
    import org.apache.spark.sql.functions._
    sourceDf.filter("status ='1' and substr(sn,2) not in ('XX','XY','XZ','YX','YY','YZ','ZX')").
      select(
        $"id".as(columns.primaryKeys(0)),
        $"whaleyAccount".as(columns.allColumns(1)),
        $"whaleyAccountName".as(columns.allColumns(2)),
        $"sn".as(columns.allColumns(3)),
        $"whaleyProductId".as(columns.allColumns(4)),
        $"whaleyProduct".as(columns.allColumns(5)),
        $"startTime".cast("timestamp").as(columns.allColumns(6)),
        $"effectiveTime".cast("timestamp").as(columns.allColumns(7)),
        $"create_time".cast("timestamp").as(columns.allColumns(8)),
        $"update_time".cast("timestamp").as(columns.allColumns(9))
    )
  }
}
