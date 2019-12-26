package cn.whaley.datawarehouse.fact.whaley



import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}
import cn.whaley.datawarehouse.fact.FactEtlBase
import cn.whaley.datawarehouse.fact.constant.LogPath
import cn.whaley.datawarehouse.util.DataExtractUtils
import org.apache.spark.sql.DataFrame

/**
  * 创建人：郭浩
  * 创建时间：2017/4/18
  * 程序作用：支付领取税换会员
  * 数据输入：helios-whaleyvip-getBuyVipProcess，用户表，账号表、订单表，发货表，商品表，会员权益表
  * 数据输出：会员操作流程
  */
object MembershipProcess extends FactEtlBase{
  topicName = "fact_whaley_membership_process"

  source = "default"

  addColumns = List(
  )

  columnsFromSource = List(
    ("current_vip_level", "current_vip_level"),
    ("firmware_version", "firmware_version"),
    ("is_yunos","is_yunos"),
    ("product_line","product_line"),
    ("process_id", "process_id"),
    ("entrance", "entrance"),
    ("page", "page"),
    ("click_button", "click_button"),
    ("process_type", "process_type"),
    ("order_id", "order_id"),
    ("pay_way", "pay_way"),
    ("process_result", "process_result"),
//    ("activity_id", "activity_id"),
//    ("popup_result", "popup_result"),
    ("dim_date", " dim_date"),
    ("dim_time", "dim_time")
  )

  dimensionColumns = List(
    new DimensionColumn("dim_whaley_membership_goods",
      List(DimensionJoinCondition(Map("goods_no" -> "goods_no"))), "membership_goods_sk"),
    new DimensionColumn("dim_whaley_product_sn",
      List(DimensionJoinCondition(Map("product_sn" -> "product_sn"))), "product_sn_sk"),
    new DimensionColumn("dim_whaley_account",
    List(DimensionJoinCondition(Map("account_id" -> "account_id"))), "account_sk")
  )

  override def readSource(startDate: String, startHour: String): DataFrame = {

    //    DataExtractUtils.readFromParquet(sqlContext,LogPath.HELIOS_WHALEYVIP_GETBUYVIPPROCESS,startDate)
    DataExtractUtils.readFromOds(sqlContext, "ods_view.log_whaleytv_main_helios_whaleyvip_getbuyvipprocess", startDate, startHour)
      .selectExpr(
            "productSN as product_sn",
            "productLine as product_line",
            "accountId as account_id",
            "currentVipLevel as current_vip_level",
            "firmwareVersion as firmware_version",
            "isYunos as is_yunos",
            "processId as process_id",
            "entrance",
            "page",
            "clickButton as click_button",
            "processType as process_type",
            "whaleyOrder as order_id",
            "payWay as pay_way",
            "processResult as process_result",
            "goodsId as goods_no",
//            "activityId as activity_id",
//            "popupResult as popup_result",
            "substr(datetime,1,10) as dim_date",
            "substr(datetime,12,8) as dim_time"
          )
  }

}
