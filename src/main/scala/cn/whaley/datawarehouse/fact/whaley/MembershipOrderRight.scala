package cn.whaley.datawarehouse.fact.whaley

import java.util.Calendar

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}
import cn.whaley.datawarehouse.fact.FactEtlBase
import cn.whaley.datawarehouse.util.{DateFormatUtils, MysqlDB}
import org.apache.spark.sql.DataFrame
/**
  * 创建人：郭浩
  * 创建时间：2017/4/18
  * 程序作用：会员订单权益表
  * 数据输入：订单表，发货表，商品表，会员权益表
  * 数据输出：会员订单权益表
  */
object MembershipOrderRight extends FactEtlBase{
  topicName = "fact_whaley_membership_order_right"

  source = "default"

  addColumns = List(

  )

  columnsFromSource = List(
    ("product_sn", "sn"),
    ("membership_account", "whaleyAccount"),
    ("membership_id", "concat_ws('_',source.sn,source.whaleyAccount)"),
    ("order_id", "whaleyOrder"),
    ("product_id", "whaleyProduct"),
    ("prime_price", "case when dim_whaley_membership_order_delivered.is_buy = 0 then 0 else source.totalPrice end  "),
    ("payment_amount", "case when dim_whaley_membership_order_delivered.is_buy = 0 then 0 else source.paymentAmount end "),
    ("duration", "duration"),
    ("duration_day", "duration_day"),
    ("dim_date", " dim_date"),
    ("dim_time", "dim_time")
  )

  dimensionsNeedInFact = List("dim_whaley_membership_order_delivered")

  dimensionColumns = List(
    new DimensionColumn("dim_whaley_membership_account_order",
      List(DimensionJoinCondition(Map("sn" -> "product_sn","whaleyOrder" -> "order_id"))), "membership_order_sk"),
    new DimensionColumn("dim_whaley_membership_order_delivered",
      List(DimensionJoinCondition(Map("sn" -> "product_sn","whaleyOrder" -> "order_id","whaleyProduct" -> "product_id"))), "membership_order_delivered_sk"),
    new DimensionColumn("dim_whaley_membership_goods",
      List(DimensionJoinCondition(Map("goodsNo" -> "goods_no"))), "membership_goods_sk"),
    new DimensionColumn("dim_whaley_membership_right",
      List(DimensionJoinCondition(Map("sn" -> "product_sn","whaleyAccount" -> "membership_account","whaleyProduct" -> "product_id"))), "membership_right_sk"),
    new DimensionColumn("dim_whaley_product_sn",
      List(DimensionJoinCondition(Map("sn" -> "product_sn"))), "product_sn_sk"),
    new DimensionColumn("dim_whaley_account",
    List(DimensionJoinCondition(Map("whaleyAccount" -> "account_id"))), "account_sk")
  )

  override def readSource(startDate: String, startHour: String): DataFrame = {


    val cal = Calendar.getInstance()
    cal.setTime(DateFormatUtils.readFormat.parse(startDate))
    val day = DateFormatUtils.cnFormat.format(cal.getTime)
    //账号订单表
    val dolphin_whaley_account_order = MysqlDB.whaleyDolphin("dolphin_whaley_account_order","id",1, 1000000000, 10)
    sqlContext.read.format("jdbc").options(dolphin_whaley_account_order).load()
      .filter("orderStutas ='1' and substr(sn,1,2) not in ('XX','XY','XZ','YX','YY','YZ','ZX')").registerTempTable("account_order")

    //账号发货订单表
    val dolphin_whaley_delivered_order = MysqlDB.whaleyDolphin("dolphin_whaley_delivered_order","id",1, 1000000000, 10)
    sqlContext.read.format("jdbc").options(dolphin_whaley_delivered_order).load()
      .filter("status = 1 and substr(sn,1,2) not in ('XX','XY','XZ','YX','YY','YZ','ZX')").registerTempTable("delivered_order")
    //开始到当天的数据
    val sql1 =
      s"""
         | select a.sn,a.whaleyAccount,a.whaleyOrder,a.goodsNo,
         |    case when b.whaleyProduct='chlid' then 'child' else b.whaleyProduct end whaleyProduct,
         |    a.totalPrice,a.paymentAmount,
         |    b.duration,
         |    b.duration_day,
         |    case when b.create_time is not null then substr(b.create_time,1,10) else substr(a.overTime,1,10) end  dim_date ,
         |    case when b.create_time is not null then substr(b.create_time,12,8) else substr(a.overTime,12,8) end  dim_time
         |  from
         |     account_order a
         |  left join
         |  delivered_order  b
         |  on a.sn = b.sn and a.whaleyOrder = b.orderId
         |    where (a.orderStatus = 4 and substr(b.create_time,1,10) <= '${day} 23:59:59')
         |    or (a.orderStatus != 4 and substr(a.overTime,1,10) <= '${day} 23:59:59')
       """.stripMargin

    //仅仅当天的数据
    val sql =
      s"""
         | select a.sn,a.whaleyAccount,a.whaleyOrder,a.goodsNo,
         |    case when b.whaleyProduct='chlid' then 'child' else b.whaleyProduct end whaleyProduct,
         |    a.totalPrice,a.paymentAmount,
         |    b.duration,
         |    b.duration_day,
         |    case when b.create_time is not null then substr(b.create_time,1,10) else substr(a.overTime,1,10) end  dim_date ,
         |    case when b.create_time is not null then substr(b.create_time,12,8) else substr(a.overTime,12,8) end  dim_time
         |  from
         |     account_order a
         |  left join
         |  delivered_order  b
         |  on a.sn = b.sn and a.whaleyOrder = b.orderId
         |  where (a.orderStatus = 4 and substr(b.create_time,1,10) = '${day}')
         |    or (a.orderStatus != 4 and substr(a.overTime,1,10) = '${day}')
       """.stripMargin

    sqlContext.sql(sql)
  }

}
