package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}
import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame

/**
  * Created by Zhu.bingxin on 17/6/21.
  *
  * 电视猫推广渠道状态维度表
  */
object PromotionChannel extends DimensionBase {

  columns.skName = "promotion_channel_sk"
  columns.primaryKeys = List(
    "promotion_channel",
    "year",
    "promotion_channel_type",
    "fee_type",
    "annual_goal"
  )
  columns.trackingColumns = List()
  columns.allColumns = List(
    "promotion_channel",
    "pri_promotion_channel_name",
    "promotion_channel_name",
    "promotion_channel_type",
    "fee_type",
    "description",
    "package_date",
    "note",
    "annual_goal",
    "year",
    "invalid_date"
  )

  sourceColumnMap = Map(

  )

  //sourceFilterWhere = "user_id is not null and user_id <> ''"
  sourceDb =  MysqlDB.dwDimensionDb("medusa_promotion_channel_status")

  override def readSource(readSourceType: SourceType.Value): DataFrame = {
    val df = super.readSource(readSourceType)
    val sourceDbMapping =  MysqlDB.dwDimensionDb("medusa_promotion_channel_mapping")
    val df_mapping = sqlContext.read.format("jdbc").options(sourceDbMapping).load()
    .filter("invalid_date is null")
    .select("promotion_channel","pri_promotion_channel_name")
    val result = df.join(df_mapping,"promotion_channel")
    //result.show(10,false)
    result
  }
  dimensionName = "dim_medusa_promotion_channel"

  fullUpdate = true

}
