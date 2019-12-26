package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame


/**
  * Created by 郭浩 on 17/4/17.
  *
  * 微鲸会员订单发货表
  */
object MemberShipOrderDelivered extends DimensionBase {
  columns.skName = "membership_order_delivered_sk"
  columns.primaryKeys = List("membership_order_delivered_id")
  columns.trackingColumns = List()
  columns.allColumns = List("membership_order_delivered_id","product_sn","order_id",
    "product_id","product_name","duration","duration_day","is_buy","delivered_time",
    "start_time","end_time","invalid_time")

  readSourceType = jdbc

  sourceDb = MysqlDB.whaleyDolphin("dolphin_whaley_delivered_order","id",1, 1000000000, 10)

  dimensionName = "dim_whaley_membership_order_delivered"

  override def filterSource(sourceDf: DataFrame): DataFrame = {
    val sq = sqlContext
    import sq.implicits._
    sourceDf.filter("status = 1 and substr(sn,1,2) not in ('XX','XY','XZ','YX','YY','YZ','ZX')").registerTempTable("tmp")

    val sql =
      s"""
         | select a.id,a.sn,a.orderId,
         | case when a.whaleyProduct='chlid' then 'child' else a.whaleyProduct end whaleyProduct,
         | case when a.whaleyProductName='基础包' then '白金会员' else a.whaleyProductName end whaleyProductName,
         | a.duration ,
         | a.duration_day,a.create_time,a.start_time,a.end_time,a.invalid_time ,
         | case when b.orderId is null or  a.whaleyProduct='diamond' then 1 else 0 end is_buy
         | from
         |  tmp a
         |  left join
         |  (select orderId from tmp group by orderId having count(1) > 1 ) b
         |  on a.orderId=b.orderId
       """.stripMargin

    sqlContext.sql(sql)
      .select(
        $"id".as(columns.allColumns(0)),
        $"sn".as(columns.allColumns(1)),
        $"orderId".as(columns.allColumns(2)),
        $"whaleyProduct".as(columns.allColumns(3)),
        $"whaleyProductName".as(columns.allColumns(4)),
        $"duration".as(columns.allColumns(5)),
        $"duration_day".as(columns.allColumns(6)),
        $"is_buy".as(columns.allColumns(7)),
        $"create_time".as(columns.allColumns(8)),
        $"start_time".as(columns.allColumns(9)),
        $"end_time".as(columns.allColumns(10)),
        $"invalid_time".as(columns.allColumns(11))
      )

  }

}
