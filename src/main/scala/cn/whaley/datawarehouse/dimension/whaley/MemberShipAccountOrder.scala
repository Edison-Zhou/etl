package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame


/**
  * Created by 郭浩 on 17/4/17.
  *
  * 微鲸会员账号订单
  */
object MemberShipAccountOrder extends DimensionBase {
  columns.skName = "membership_order_sk"
  columns.primaryKeys = List("membership_order_id")
  columns.trackingColumns = List()
  columns.allColumns = List("membership_order_id","product_sn","membership_account",
    "order_id","order_status","payment_type","order_type","goods_no","prime_price",
    "payment_amount","pay_method","pay_is_yunos","foreign_key","over_time","create_time")

  readSourceType = jdbc

  sourceDb = MysqlDB.whaleyDolphin("dolphin_whaley_account_order","id",1, 1000000000, 10)

  dimensionName = "dim_whaley_membership_account_order"

  sourceTimeCol = "updateTime"

  override def filterSource(sourceDf: DataFrame): DataFrame = {
    val sq = sqlContext

    import sq.implicits._
    sourceDf.filter("orderStutas ='1' and substr(sn,1,2) not in ('XX','XY','XZ','YX','YY','YZ','ZX')")
        .selectExpr(
          "id as membership_order_id",
          "sn as product_sn",

          "whaleyAccount as membership_account",
          "whaleyOrder as order_id",
          "orderStatus as order_status",
          "paymentType as payment_type",
          "orderTypeId as order_type",
          "goodsNo as goods_no",
          "totalPrice as prime_price",
          "paymentAmount as payment_amount",
          "payMethod as pay_method",
          "(case when payMethod like '%_yun' then '1' else '0' end ) as  pay_is_yunos",
          "foreignKey as foreign_key",
          "overTime as over_time",
          "createTime as create_time"
        )
/*
    sourceDf.filter("orderStutas ='1' and substr(sn,2) not in ('XX','XY','XZ','YX','YY','YZ','ZX')")
      .select(
        $"id".as(columns.allColumns(0)),
        $"sn".as(columns.allColumns(1)),
        $"whaleyAccount".as(columns.allColumns(2)),
        $"whaleyOrder".as(columns.allColumns(3)),
        $"orderStatus".as(columns.allColumns(4)),
        $"paymentType".as(columns.allColumns(5)),
        $"orderTypeId".as(columns.allColumns(6)),
        $"goodsNo".as(columns.allColumns(7)),
        $"totalPrice".as(columns.allColumns(8)),
        $"paymentAmount".as(columns.allColumns(9)),
        $"payMethod".as(columns.allColumns(10)),
        $"foreignKey".as(columns.allColumns(11)),
        $"overTime".cast("timestamp").as(columns.allColumns(12)),
        $"createTime".cast("timestamp").as(columns.allColumns(13))
      )*/
  }

}
