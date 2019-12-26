package cn.whaley.datawarehouse.fact.moretv

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition, UserDefinedColumn}
import cn.whaley.datawarehouse.fact.FactEtlBase
import cn.whaley.datawarehouse.global.SourceType._
import org.apache.spark.sql.functions._


/**
  * Created by Tony on 17/4/5.
  */
object UserLogin extends FactEtlBase {

  //  debug = true

  topicName = "fact_medusa_user_login"

  source = "default"

  readSourceType = ods

  odsTableName = "ods_view.log_medusa_main3x_loginlog"

  addColumns = List(
    UserDefinedColumn("ipKey", udf(getIpKey: String => Long), List("ip")),
    UserDefinedColumn("dim_date", udf(getDimDate: String => String), List("datetime")),
    UserDefinedColumn("dim_time", udf(getDimTime: String => String), List("datetime")),
    UserDefinedColumn("app_series", udf(getAppSeries: String => String), List("version")),
    UserDefinedColumn("app_version", udf(getAppVersion: String => String), List("version"))

  )

  partition = 180

  columnsFromSource = List(
    ("product_serial", "productSerial"),
    ("sys_ver", "systemVersion"),
    ("wifi_mac", "wifiMac"),
    ("app_name", "appName"),
    ("ip", "ip"),
    ("mac", "mac"),
    ("product_model", "productModel"),
    ("product_version", "productVersion"),
    ("promotion_channel", "promotionChannel"),
    ("sn", "sn"),
    ("log_timestamp", "timestamp"),
    ("user_id", "userId"),
    ("user_type", "userType"),
    ("version", "version"),
    ("dim_date", "dim_date"),
    ("dim_time", "dim_time")
  )

  dimensionColumns = List(
    new DimensionColumn("dim_web_location",
      List(DimensionJoinCondition(Map("ipKey" -> "web_location_key"))), "web_location_sk"),
    new DimensionColumn("dim_medusa_terminal_user",
      List(
        DimensionJoinCondition(Map("userId" -> "user_id"))
      ),
      "user_sk"),
    new DimensionColumn("dim_medusa_product_model",
      List(DimensionJoinCondition(Map("productModel" -> "product_model"))), "product_model_sk"),
    new DimensionColumn("dim_medusa_promotion",
      List(DimensionJoinCondition(Map("promotionChannel" -> "promotion_code"))), "promotion_sk"),

    new DimensionColumn("dim_app_version",
      List(DimensionJoinCondition(Map("app_series" -> "app_series", "app_version" -> "version")))
      , "app_version_sk")
  )


  def getIpKey(ip: String): Long = {
    try {
      val ipInfo = ip.split("\\.")
      if (ipInfo.length >= 3) {
        (((ipInfo(0).toLong * 256) + ipInfo(1).toLong) * 256 + ipInfo(2).toLong) * 256
      } else 0
    } catch {
      case ex: Exception => 0
    }
  }

  def getDimDate(dateTime: String): String = {
    try {
      val dateTimeInfo = dateTime.split(" ")
      if (dateTimeInfo.length >= 2) {
        dateTimeInfo(0)
      } else ""
    } catch {
      case ex: Exception => ""
    }
  }

  def getDimTime(dateTime: String): String = {
    try {
      val dateTimeInfo = dateTime.split(" ")
      if (dateTimeInfo.length >= 2) {
        dateTimeInfo(1)
      } else ""
    } catch {
      case ex: Exception => ""
    }
  }

  def getAppSeries(seriesAndVersion: String): String = {
    try {
      val index = seriesAndVersion.lastIndexOf("_")
      if (index > 0) {
        seriesAndVersion.substring(0, index)
      } else ""
    } catch {
      case ex: Exception => ""
    }
  }

  def getAppVersion(seriesAndVersion: String): String = {
    try {
      val index = seriesAndVersion.lastIndexOf("_")
      if (index > 0) {
        seriesAndVersion.substring(index + 1, seriesAndVersion.length)
      } else ""
    } catch {
      case ex: Exception => ""
    }
  }

}
