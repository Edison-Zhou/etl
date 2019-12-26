package cn.whaley.datawarehouse.fact.moretv

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition, Udf, UserDefinedColumn}
import cn.whaley.datawarehouse.fact.FactEtlBase
import cn.whaley.datawarehouse.util.{DataExtractUtils, DateFormatUtils}
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * Created by lituo on 2017/9/20.
  */
object ProgramClick extends FactEtlBase {

  topicName = "fact_medusa_program_click"

  source = "main3x"

  addColumns = List(
    UserDefinedColumn("dim_date", udf(Udf.getDimDate: String => String), List("datetime")),
    UserDefinedColumn("dim_time", udf(Udf.getDimTime: String => String), List("datetime"))
  )

  columnsFromSource = List(
    ("user_id", "userId"),
    ("city_level", "cityLevel"),
    ("location_index", "locationIndex"),
    ("content_type", "contentType"),
    ("link_value", "linkValue"),
    ("biz", "biz"),
    ("alg", "alg"),
    ("page", "page"),
    ("area", "area"),
    ("location_code", "locationCode"),
    ("dim_date", "dim_date"),
    ("dim_time", "dim_time")
  )

  dimensionColumns = List(

    /** 页面维度 */
    new DimensionColumn("dim_medusa_page",
      List(DimensionJoinCondition(
        //关联页面和区域和位置
        Map("page" -> "page_code", "area" -> "area_code", "locationCode" -> "location_code")),
        DimensionJoinCondition(
          //关联页面和区域
          Map("page" -> "page_code", "area" -> "area_code"), "location_code is null"),
        DimensionJoinCondition(
          //关联页面和区域中文名
          Map("page" -> "page_code", "area" -> "area_name"), "location_code is null"),
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
      "account_sk"),

    /** 获得节目维度program_sk */
    new DimensionColumn("dim_medusa_program",
      List(DimensionJoinCondition(Map("linkValue" -> "sid"))),
      "program_sk")
  )

  val medusaContentType: String = Array("movie", "tv", "hot", "zongyi", "comic", "jilu", "xiqu", "sports", "interest", "game").mkString("|")
  val channel_regex = s"^home\\*(my_tv|classification)\\*($medusaContentType)-($medusaContentType)\\*([a-zA-Z0-9&\\u4e00-\\u9fa5]+).*"
  val subject_regex = "^([a-zA-Z]+)([0-9]+)$"
  val entrance_regex = "^([a-zA-Z]+_?)+$"

  override def readSource(startDate: String, startHour: String): DataFrame = {
    sqlContext.udf.register("getPageFromPath", getPageFromPath _)
    sqlContext.udf.register("getAccessAreaFromPath", getAccessAreaFromPath _)
    sqlContext.udf.register("getLocationCode", getLocationCode _)
    sqlContext.udf.register("getLinkValue", getLinkValue _)

    val date = DateUtils.addDays(DateFormatUtils.readFormat.parse(startDate), 1)
    val realStartDate = DateFormatUtils.readFormat.format(date)

    val metaFields = List("userId", "accountId", "promotionChannel", "productModel", "apkSeries", "apkVersion",
      "buildDate", "datetime", "ip", "cityLevel")

    //    val launcherClickOriginDf = DataExtractUtils.readFromParquet(sqlContext, LogPath.MEDUSA_LAUNCHER_CLICK, realStartDate)
    val launcherClickOriginDf = DataExtractUtils.readFromOds(sqlContext, "ods_view.log_medusa_main3x_homeaccess", startDate, startHour)

    //首页点击
    val launcherClickDf = launcherClickOriginDf.selectExpr(metaFields ++ List("alg", "biz",
      "locationIndex", "accessArea as area", "getLocationCode(accessLocation) as locationCode",
      "getLinkValue(accessLocation) as linkValue", "null as contentType", "'launcher' as page"): _*)

    //    val detailOriginDf = DataExtractUtils.readFromParquet(sqlContext, LogPath.MEDUSA_DETAIL, realStartDate)
    val detailOriginDf = DataExtractUtils.readFromOds(sqlContext, "ods_view.log_medusa_main3x_detail", startDate, startHour)
    detailOriginDf.persist()

    //列表页点击
    val listClickDf = detailOriginDf.selectExpr(metaFields ++ List("null as alg", "null as biz",
      "null as locationIndex", "getAccessAreaFromPath(pathMain) as area",
      "null as locationCode",
      "videoSid as linkValue", "contentType",
      "getPageFromPath(pathMain, pathSub) as page"): _*)
      .where("page is not null")

    //相似影片点击
    val similarClickDf = detailOriginDf.where("pathSub like 'similar%'")
      .selectExpr(metaFields ++ List("null as alg", "null as biz",
        "null as locationIndex", "'similar' as area",
        "null as locationCode",
        "videoSid as linkValue", "contentType",
        "'detail' as page"): _*)

    //长视频退出推荐
    val playBackDf = detailOriginDf.where("pathSub like 'peoplealsolike%'")
      .selectExpr(metaFields ++ List("null as alg", "null as biz",
        "null as locationIndex", "null as area",
        "null as locationCode",
        "videoSid as linkValue", "contentType",
        "'play_back' as page"): _*)

    //    val playOriginDf = DataExtractUtils.readFromParquet(sqlContext, LogPath.MEDUSA_PLAY, realStartDate)
    val playOriginDf = DataExtractUtils.readFromOds(sqlContext, "ods_view.log_medusa_main3x_play", startDate, startHour)

    //短视频退出推荐
    val hotBackClick = playOriginDf.where("pathSub like 'guessyoulike%'")
      .selectExpr(metaFields ++ List("alg", "biz",
        "null as locationIndex", "null as area",
        "null as locationCode",
        "videoSid as linkValue", "contentType",
        "'play_back' as page"): _*)

    launcherClickDf.unionAll(listClickDf).unionAll(hotBackClick).unionAll(playBackDf).unionAll(similarClickDf)

  }

  def getPageFromPath(pathMain: String, pathSub: String): String = {
    if (pathMain == null) {
      return null
    }
    channel_regex.r findFirstMatchIn pathMain match {
      case Some(p) => {
        return "list_" + p.group(2)
      }
      case None =>
    }

    if (pathSub != null && pathSub.indexOf("peoplealsolike") > -1) {
      "play_back"
    } else {
      null
    }
  }

  def getAccessAreaFromPath(pathMain: String): String = {
    if (pathMain == null) {
      return null
    }
    channel_regex.r findFirstMatchIn pathMain match {
      case Some(p) => {
        p.group(4)
      }
      case None =>
        null
    }
  }

  def getLocationCode(accessLocation: String): String = {
    if (accessLocation != null && accessLocation.matches(entrance_regex)) {
      if (accessLocation.startsWith("site_"))
        accessLocation.substring(5)
      else if (accessLocation.equals("site_sport") || accessLocation.equals("sport"))
        "sports"
      else accessLocation
    } else {
      null
    }
  }

  //专题或者节目
  def getLinkValue(accessLocation: String): String = {
    if (accessLocation != null && accessLocation.matches(entrance_regex)) {
      null
    } else {
      accessLocation
    }
  }


}
