package cn.whaley.datawarehouse.fact.moretv

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition, Udf, UserDefinedColumn}
import cn.whaley.datawarehouse.fact.FactEtlBase
import cn.whaley.datawarehouse.fact.constant.LogPath
import cn.whaley.datawarehouse.util.{DataExtractUtils, DateFormatUtils}
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by lituo on 2017/9/20.
  */
object PageExpose extends FactEtlBase {

  topicName = "fact_medusa_page_expose"

  source = "main3x"

  addColumns = List(
    UserDefinedColumn("ipKey", udf(Udf.getIpKey: String => Long), List("ip")),
    UserDefinedColumn("dim_date", udf(Udf.getDimDate: String => String), List("datetime")),
    UserDefinedColumn("dim_time", udf(Udf.getDimTime: String => String), List("datetime"))
  )

  columnsFromSource = List(
    ("user_id", "userId"),
    ("city_level", "cityLevel"),
    ("area", "area"),
    ("content_type", "contentType"),
    ("expose_sid_list", "sidList"),
    ("expose_page_num", "expose_page_num"),
    ("expose_type", "expose_type"),
    ("biz", "biz"),
    ("alg", "alg"),
    ("dim_date", "dim_date"),
    ("dim_time", "dim_time")
  )

  dimensionColumns = List(
    /** 页面维度 */
    new DimensionColumn("dim_medusa_page",
      List(DimensionJoinCondition(
        //关联页面和区域
        Map("page" -> "page_code", "area" -> "area_code"), "location_code is null"),
        DimensionJoinCondition(
          //只是用区域关联
          Map("area" -> "area_code"), "location_code is null"),
        DimensionJoinCondition(
          //只关联页面
          Map("page" -> "page_code"), "location_code is null and area_code is null", null, "area is null or area = ''")),
      "page_sk"),

    /** 获得用户维度user_sk */
    new DimensionColumn("dim_medusa_terminal_user",
      List(DimensionJoinCondition(Map("userId" -> "user_id"))),
      List(("user_sk", "user_sk"), ("web_location_sk", "user_web_location_sk"))),

    /** 获得设备型号维度product_model_sk */
    new DimensionColumn("dim_medusa_product_model",
      List(DimensionJoinCondition(Map("productModel" -> "product_model"))),
      "product_model_sk"),

    /** 获得推广渠道维度promotion_sk */
    new DimensionColumn("dim_medusa_promotion",
      List(DimensionJoinCondition(Map("promotionChannel" -> "promotion_code"))),
      "promotion_sk"),

    /** 获得app版本维度app_version_sk */
    new DimensionColumn("dim_app_version",
      List(
        DimensionJoinCondition(Map("apkSeries" -> "app_series", "apkVersion" -> "version", "buildDate" -> "build_time")),
        DimensionJoinCondition(Map("apkSeries" -> "app_series", "apkVersion" -> "version"))
      ),
      "app_version_sk"),

    /** 获得账号维度account_sk */
    new DimensionColumn("dim_medusa_account",
      List(DimensionJoinCondition(Map("accountId" -> "account_id"))),
      "account_sk")
  )

  override def readSource(startDate: String, startHour: String): DataFrame = {
    val date = DateUtils.addDays(DateFormatUtils.readFormat.parse(startDate), 1)
    val realStartDate = DateFormatUtils.readFormat.format(date)

    val metaFields = List("userId", "accountId", "promotionChannel", "productModel", "apkSeries", "apkVersion",
      "buildDate", "datetime", "ip", "cityLevel")

    //频道页和详情页曝光
    //    var channelExposeDf = DataExtractUtils.readFromParquet(sqlContext, LogPath.MEDUSA_CHANNEL_EXPOSE, realStartDate)
    var channelExposeDf = DataExtractUtils.readFromOds(sqlContext, "ods_view.log_medusa_main3x_medusa_channel_expose", startDate, startHour)

    channelExposeDf = channelExposeDf.selectExpr(metaFields ++ List("alg", "biz", "area", "contentType",
      "split(videoSid, ',') as sidList",
      " cast ((case when expose_num <= 1 then 1 else expose_num/2 end) as long) as expose_page_num", "expose_type",
      "case when videoSid is null or trim(videoSid) = '' then concat('list_',contentType)  else 'detail' end as page"): _*)


    //首页曝光
    //    var launcherExposeDf = DataExtractUtils.readFromParquet(sqlContext, LogPath.MEDUSA_LAUNCHER_EXPOSE, realStartDate)
    var launcherExposeDf = DataExtractUtils.readFromOds(sqlContext, "ods_view.log_medusa_main3x_medusa_launcher_expose", startDate, startHour)


    launcherExposeDf = launcherExposeDf.selectExpr(metaFields ++ List("alg", "biz", "area", "'home' as contentType",
      "null as sidList",
      "cast (1 as long) as expose_page_num", "null as expose_type", "'launcher' as page"): _*)
      .withColumn("area", expr("case when area = 'shortVideo' then 'hotSubject' " +
        "when area = 'personalTailor' then 'taste' else area end"))


    //视频退出播放页曝光
    //    var playBackExposeDf = DataExtractUtils.readFromParquet(sqlContext, LogPath.MEDUSA_PLAY_BACK, realStartDate)
    var playBackExposeDf = DataExtractUtils.readFromOds(sqlContext, "ods_view.log_medusa_main3x_medusa_play_back", startDate, startHour)

    playBackExposeDf = playBackExposeDf.selectExpr(metaFields ++ List("alg", "biz", "null as area", "contentType", "sidList",
      "cast (1 as long) as expose_page_num", "null as expose_type", "'play_back' as page"): _*)

    channelExposeDf.unionAll(launcherExposeDf).unionAll(playBackExposeDf)

  }


}
