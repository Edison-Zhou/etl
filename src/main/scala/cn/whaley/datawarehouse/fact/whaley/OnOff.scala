package cn.whaley.datawarehouse.fact.whaley

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}
import cn.whaley.datawarehouse.fact.FactEtlBase
import cn.whaley.datawarehouse.fact.constant.LogPath
import cn.whaley.datawarehouse.util.DataExtractUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType,DataType}

/**
  * 创建人：郭浩
  * 创建时间：2017/5/03
  * 程序作用：开关机日子
  * 数据输入：on/off日志
  * 数据输出：开关机事实表
  */
object OnOff extends FactEtlBase{
  topicName = "fact_whaley_on_off"

  source = "default"

  addColumns = List(
  )

  columnsFromSource = List(
    ("current_vip_level", "current_vip_level"),
    ("firmware_version", "firmware_version"),
    ("is_yunos","is_yunos"),
    ("product_line","product_line"),
    ("log_type","log_type"),
    ("event","event"),
    ("mode","mode"),
    ("duration","duration"),
    ("start_time","cast(start_time as long ) "),
    ("end_time","cast(end_time as long ) "),
    ("dim_date", " dim_date"),
    ("dim_time", "dim_time")
  )
  dimensionColumns = List(
    new DimensionColumn("dim_whaley_product_sn",
      List(DimensionJoinCondition(Map("product_sn" -> "product_sn"))),
      List(("product_sn_sk", "product_sn_sk"), ("web_location_sk", "user_web_location_sk"))),
    new DimensionColumn("dim_whaley_account",
    List(DimensionJoinCondition(Map("account_id" -> "account_id"))), "account_sk")
  )

  override def readSource(startDate: String, startHour: String): DataFrame = {
    val fields = List(
      ("productSN",null,StringType),
      ("productLine",null,StringType),
      ("accountId",null,StringType),
      ("currentVipLevel",null,StringType),
      ("firmwareVersion",null,StringType),
      ("isYunos",null,StringType),
      ("event",null,StringType),
      ("mode",null,StringType),
      ("duration",0,IntegerType),
      ("startTime",0,IntegerType),
      ("endTime",0,IntegerType),
      ("datetime",null,StringType)
    )

    //    var onDf = DataExtractUtils.readFromParquet(sqlContext,LogPath.HELIOS_ON,startDate)
    //    var offDf = DataExtractUtils.readFromParquet(sqlContext,LogPath.HELIOS_OFF,startDate)

    var onDf = DataExtractUtils.readFromOds(sqlContext, "ods_view.log_whaleytv_main_on", startDate, startHour)
    var offDf = DataExtractUtils.readFromOds(sqlContext, "ods_view.log_whaleytv_main_off", startDate, startHour)

    onDf = addColumn(onDf,fields)
    offDf = addColumn(offDf,fields)

    //on,off日志合并
    onDf.selectExpr(
            "productSN as product_sn",
            "productLine as product_line",
            "accountId as account_id",
            "currentVipLevel as current_vip_level",
            "firmwareVersion as firmware_version",
            "isYunos as is_yunos",
            " 'on' as log_type ",
            "event",
            "mode",
            "0 as duration",
            "0 as start_time",
            "0 as end_time",
            "substr(datetime,1,10) as dim_date",
            "substr(datetime,12,8) as dim_time"
          )
      .unionAll(
         offDf.selectExpr(
                "productSN as product_sn",
                "productLine as product_line",
                "accountId as account_id",
                "currentVipLevel as current_vip_level",
                "firmwareVersion as firmware_version",
                "isYunos as is_yunos",
                " 'off' as log_type ",
                "event",
                "mode",
                "case when duration is null then 0 else duration end  duration",
                "case when startTime is null then 0 else startTime end start_time",
                "case when endTime is null then 0 else endTime end end_time",
                "substr(datetime,1,10) as dim_date",
                "substr(datetime,12,8) as dim_time"
              )
    )

  }

  def addColumn(df:DataFrame,fields:List[(String,Any,DataType)]):DataFrame = {
    var dataFrame:DataFrame = df
    fields.foreach(tuple=>{
      val field =  tuple._1
      val value = tuple._2
      val dataType = tuple._3
      val flag = dataFrame.schema.fieldNames.contains(field)
      if(!flag){
        dataFrame = dataFrame.withColumn(field,lit(value).cast(dataType))
      }
    })
    dataFrame
  }

}
