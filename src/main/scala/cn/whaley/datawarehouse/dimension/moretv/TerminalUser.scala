package cn.whaley.datawarehouse.dimension.moretv

import java.text.SimpleDateFormat
import java.util.Date

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition, UserDefinedColumn}
import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global
import cn.whaley.datawarehouse.util.{DataExtractUtils, MysqlDB}
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{udf, _}


/**
  * Created by Tony on 17/3/10.
  *
  * 电视猫用户维度表
  */
object TerminalUser extends DimensionBase {

  columns.skName = "user_sk"
  columns.primaryKeys = List("user_id")
  columns.trackingColumns = List()
  columns.allColumns = List(
    "user_id",
    "open_time",
    "origin_ip",
    "ip",
    "mac",
    "wifi_mac",
    "product_model",
    "product_serial",
    "promotion_channel",
    "system_version",
    "user_type",
    "app_series",
    "app_version",
    "current_version"
  )

  sourceColumnMap = Map(
    "open_time" -> "openTime",
    "user_type" -> "userType"
  )

  sourceFilterWhere = "user_id is not null and user_id <> ''"
  //  sourceDb = MysqlDB.medusaTvServiceAccount

  columns.addColumns = List(
    UserDefinedColumn("ip_key", udf(getIpKey: String => Long), List("ip")))

  columns.linkDimensionColumns = List(
    new DimensionColumn(
      "dim_web_location",
      List(DimensionJoinCondition(Map("ip_key" -> "web_location_key"))),
      "web_location_sk"
    ),
    new DimensionColumn("dim_app_version",
      List(DimensionJoinCondition(
        Map("app_series" -> "app_series", "app_version" -> "version"),
        null,
        List(("build_time", false)))),
      "app_version_sk")
  )

  dimensionName = "dim_medusa_terminal_user"

  sourceTimeCol = "lastLoginTime"

  override def readSource(readSourceType: global.SourceType.Value): DataFrame = {
    val date = new SimpleDateFormat("yyyyMMdd").format(DateUtils.addDays(new Date(), -1))
    DataExtractUtils.readFromOds(sqlContext, "ods_view.db_snapshot_mysql_medusa_mtv_account", date, "00").
      select("user_id",
        "openTime",
        "mac",
        "ip",
        "wifi_mac",
        "product_model",
        "product_serial",
        "promotion_channel",
        "system_version",
        "userType",
        "current_version")
  }

  override def filterSource(sourceDf: DataFrame): DataFrame = {

    val udFunctionSeries = udf(getAppSeries: String => String)
    val udFunctionVersion = udf(getAppVersion: String => String)

    val newDf = sourceDf.withColumn("app_version", udFunctionVersion(col("current_version")))
      .withColumn("app_series", udFunctionSeries(col("current_version")))
      .withColumnRenamed("ip", "origin_ip")

    //长连接数据
    val date = new SimpleDateFormat("yyyyMMdd").format(DateUtils.addDays(new Date(), -1))
    val accountDf = DataExtractUtils.readFromOds(sqlContext, "ods_view.db_snapshot_mysql_medusa_account", date, "00")
//    val accountSource = MysqlDB.lcmsAccount
//    val accountDf = sqlContext.read.format("jdbc").options(accountSource).load()
      .where("product = 'moretv'").selectExpr("uid", "real_client_ip").dropDuplicates(List("uid"))

    val df = newDf.as("a").join(accountDf.as("b"), expr("a.user_id = b.uid"), "leftouter")
      .selectExpr("a.*", "case when b.real_client_ip is null or trim(real_client_ip) = '' " +
        "then a.origin_ip else b.real_client_ip end as ip")

    super.filterSource(df)

  }

  def getAppSeries(seriesAndVersion: String): String = {
    try {
      val index = seriesAndVersion.lastIndexOf("_")
      if (index > 0) {
        seriesAndVersion.substring(0, index)
      } else null
    } catch {
      case ex: Exception => null
    }
  }

  def getAppVersion(seriesAndVersion: String): String = {
    try {
      val index = seriesAndVersion.lastIndexOf("_")
      if (index > 0) {
        seriesAndVersion.substring(index + 1, seriesAndVersion.length)
      } else null
    } catch {
      case ex: Exception => null
    }
  }

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
}
