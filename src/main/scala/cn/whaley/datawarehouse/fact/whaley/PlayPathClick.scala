package cn.whaley.datawarehouse.fact.whaley

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition, UserDefinedColumn}
import cn.whaley.datawarehouse.fact.FactEtlBase
import cn.whaley.datawarehouse.fact.constant.LogPath
import cn.whaley.datawarehouse.fact.whaley.util._
import cn.whaley.datawarehouse.util.DataExtractUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}

/**
  * Created by huanghu on 17/7/20.
  * modify by qizhen on 18/6/20.
  */
object PlayPathClick extends FactEtlBase {

  topicName = "fact_whaley_play_path_click"

  source = "default"

  addColumns = List(
    //WUI版本
    UserDefinedColumn("udc_wui_version", udf(LauncherEntranceUtils.wuiVersionFromPlay: (String, String) => String),
      List("rom_version", "firmware_version")),
    //首页的Area
    UserDefinedColumn("udc_launcher_access_area",
      udf(LauncherEntranceUtils.getLauncherAreaFromClick: (String,String,String, String, String, String,String) => String),
      List("rom_version", "firmware_version", "page", "area_name", "location_code", "location_index", "flag")),
    //首页的Area（vod2.0）
    UserDefinedColumn("udc_launcher_access_area_vod2",
      udf(LauncherEntranceUtils.launcherAccessAreaFromPlayVod: (String,String) => String),
      List("location_code","flag")),
    //首页的location
    UserDefinedColumn("udc_launcher_access_location",
      udf(LauncherEntranceUtils.getLauncherLocationFromClick: (String,String,String, String, String,  String, String,String) => String),
      List("rom_version", "firmware_version", "page", "area_name", "location_code", "link_value","location_index","flag")),
    //首页的location（vod2.0）
    UserDefinedColumn("udc_launcher_access_location_vod2",
      udf(LauncherEntranceUtils.launcherAccessLocationFromPlayVod: (String,String,String) => String),
      List("location_code", "link_value", "flag")),
    //首页的索引值
    UserDefinedColumn("udc_launcher_location_index",
      udf(LauncherEntranceUtils.launcherLocationIndexFromClick: (String, String, String, String, String,String) => Int),
      List("page", "rom_version", "firmware_version", "area_name", "location_index", "flag")),
    //首页的索引值（vod2.0）
    UserDefinedColumn("udc_launcher_location_index_vod2",
      udf(LauncherEntranceUtils.launcherLocationIndexFromPlayVod: (String, String, String,String) => Int),
      List("area_name", "location_code", "location_index","flag")),

    //主页的location
    UserDefinedColumn("udc_page_location_code",
      udf(ChannelLauncherEntranceUtils.getPageLocationFromClick: (String, String) => String),
      List("page", "location_code")),

    //主页的索引
    UserDefinedColumn("udc_page_location_index",
      udf(ChannelLauncherEntranceUtils.getPageLocationIndexFromClick: (String) => Int),
      List("location_index")),

    //主页的areaCode
    UserDefinedColumn("udc_page_area_code",
      udf(ChannelLauncherEntranceUtils.getPageAreaFromClick: (String,String) => String),
      List("page","area_name")),


    UserDefinedColumn("dim_date", udf(getDimDate: String => String), List("date_time")),
    UserDefinedColumn("dim_time", udf(getDimTime: String => String), List("date_time"))
  )

  columnsFromSource = List(
    ("rom_version", "rom_version"),
    ("firmware_version", "firmware_version"),
    ("product_line", "product_line"),
    ("product_sn","product_sn"),
    ("page", "page"),
    ("link_type", "cast(link_type as BIGINT)"),
    ("link_value", "link_value"),
    ("ad_putting_id", " cast(ad_putting_id as BIGINT)"),
    ("position_type", "position_type"),
    ("data_source","data_source"),
    ("recommend_type","recommend_type"),
    ("dim_date", " dim_date"),
    ("dim_time", "dim_time")

  )


  dimensionColumns = List(

    new DimensionColumn("dim_whaley_product_sn",
      List(DimensionJoinCondition(Map("product_sn" -> "product_sn"))),
      List(("product_sn_sk", "product_sn_sk"), ("web_location_sk", "user_web_location_sk"))),

    //账号
    new DimensionColumn("dim_whaley_account",
      List(DimensionJoinCondition(Map("account_id" -> "account_id"))), "account_sk"),

    //首页入口
    new DimensionColumn("dim_whaley_launcher_entrance",
      List(
        DimensionJoinCondition(
          Map("udc_wui_version" -> "launcher_version",
            "udc_launcher_access_location_vod2" -> "access_location_code",
            "udc_launcher_access_area_vod2" -> "access_area_code",
            "udc_launcher_location_index_vod2" -> "launcher_location_index")),
        DimensionJoinCondition(
          Map("udc_wui_version" -> "launcher_version",
            "udc_launcher_access_area_vod2" -> "access_area_code",
            "udc_launcher_access_location_vod2" -> "access_location_code"),
          "launcher_location_index = -1"),
        DimensionJoinCondition(
        Map("udc_wui_version" -> "launcher_version",
          "udc_launcher_access_location" -> "access_location_code",
          "udc_launcher_access_area" -> "access_area_code",
          "udc_launcher_location_index" -> "launcher_location_index")),
        DimensionJoinCondition(
          Map("udc_wui_version" -> "launcher_version",
            "udc_launcher_access_area" -> "access_area_code",
            "udc_launcher_access_location" -> "access_location_code"),
            "launcher_location_index = -1")
      ), "launcher_entrance_sk"),


    //频道页入口
    new DimensionColumn("dim_whaley_page_entrance",
      List(DimensionJoinCondition(
        Map("page" -> "page_code",
          "udc_page_area_code" -> "area_code",
          "location_code" ->"location_code",
          "udc_page_location_index" -> "location_index")),
        DimensionJoinCondition(
          Map("page" -> "page_code",
            "udc_page_area_code" -> "area_code",
            "udc_page_location_index" -> "location_index"),"location_code is null")
      ), "page_entrance_sk"),

    //节目聚合维度
    new DimensionColumn("dim_whaley_link_type",
      List(
        DimensionJoinCondition(
          Map("sid" -> "link_value_code",
            "link_type" -> "link_type_code")
        )), "link_type_sk"),

    //专题信息
    new DimensionColumn("dim_whaley_subject",
      List(DimensionJoinCondition(Map("sid" -> "subject_code"))), "subject_sk"),

    //节目信息
    new DimensionColumn("dim_whaley_program",
      List(DimensionJoinCondition(Map("sid" -> "sid"))), "program_sk")
  )

  override def readSource(startDate: String, startHour: String): DataFrame = {


    val fields = List(
      ("linkType", "-1", StringType),
      ("adPuttingId", "-1", StringType),
      ("linkValue", null, StringType),
      ("tableName", null, StringType),
      ("elementCode", null, StringType),
      ("positionIndex", null, StringType),
      ("positionType", null, StringType),
      ("positionArea", null, StringType),
      ("buttonType", null, StringType),
      ("dataSource", null, StringType),
      ("recommendType" ,null, StringType)
    )

    val fields1 = List(
      ("linkType", "-1", StringType),
      ("adPuttingId", "-1", StringType),
      ("linkValue", null, StringType),
      ("tableName", null, StringType),
      ("elementCode", null, StringType),
      ("positionIndex", null, StringType),
      ("positionType", null, StringType),
      ("positionArea", null, StringType),
      ("buttonType", null, StringType),
      ("pageType", "home", StringType),
      ("dataSource", null, StringType),
      ("recommendType" ,null, StringType)
    )

    val fields2 = List(
      ("adPuttingId", "-1", StringType),
      ("positionType", null, StringType)
    )


    // val launcher = s"/log/whaley/parquet/$startDate/launcher"
    // val wui = s"/log/boikgpokn78sb95kjhfrendoj8ilnoi7/parquet/$startDate/positionClick"


    //      var getWuiLauncherInfo = DataExtractUtils.readFromParquet(sqlContext, LogPath.HELIOS_WUI_LAUNCHER, startDate)
    //      var getLauncherInfo = DataExtractUtils.readFromParquet(sqlContext, LogPath.HELIOS_LAUNCHER, startDate)
    //      var getChannelInfo = DataExtractUtils.readFromParquet(sqlContext, LogPath.HELIOS_CHANNEL_Click, startDate)
    //      var getMovieInfo = DataExtractUtils.readFromParquet(sqlContext, LogPath.HELIOS_MOVIE_Click, startDate)
    //      var getWuiButton = DataExtractUtils.readFromParquet(sqlContext, LogPath.HELIOS_WUI_BUTTON, startDate)
    var getWuiLauncherInfo = DataExtractUtils.readFromOds(sqlContext, "ods_view.log_whaleytv_main_positionclick", startDate, startHour)
    var getLauncherInfo = DataExtractUtils.readFromOds(sqlContext, "ods_view.log_whaleytv_main_launcher", startDate, startHour)
    var getVod20LauncherInfo = DataExtractUtils.readFromOds(sqlContext, "ods_view.log_whaleytv_main_path_launcher_click", startDate, startHour)
    var getChannelInfo = DataExtractUtils.readFromOds(sqlContext, "ods_view.log_whaleytv_main_helios_channelhome_click", startDate, startHour)
    var getMovieInfo = DataExtractUtils.readFromOds(sqlContext, "ods_view.log_whaleytv_main_helios_whaleymovie_moviehomeaccess", startDate, startHour)
    var getWuiButton = DataExtractUtils.readFromOds(sqlContext, "ods_view.log_whaleytv_main_buttonclick", startDate, startHour)
        .where("buttonType = 'signal' or buttonType = 'search' ")


      getWuiLauncherInfo = addColumn(getWuiLauncherInfo, fields)
      getLauncherInfo = addColumn(getLauncherInfo, fields1)
      getVod20LauncherInfo = addColumn(getVod20LauncherInfo, fields2)
      getChannelInfo = addColumn(getChannelInfo, fields)
      getMovieInfo = addColumn(getMovieInfo, fields)
      getWuiButton = addColumn(getWuiButton, fields)

      getWuiLauncherInfo.selectExpr(
        "romVersion as rom_version",
        "firmwareVersion as firmware_version",
        "productLine as product_line",
        "productSN as product_sn",
        "pageType as page",
        "case when positionArea='我的电视' then positionArea else tableCode end as area_name",
        "elementCode as  location_code",
        "linkValue as link_value",
        "linkValue as  sid ",
        "positionIndex as location_index",
        "linkType as link_type",
        "adPuttingId as ad_putting_id",
        "positionType as position_type",
        "accountId as account_id",
        "dataSource as data_source",
        "recommendType as recommend_type",
        "datetime as date_time",
        "'one' as flag"
      ).unionAll(
        getLauncherInfo.selectExpr(
          "romVersion as rom_version",
          "firmwareVersion as firmware_version",
          "productLine as product_line",
          "productSN as product_sn",
          "pageType as page",
          "accessAera  as   area_name",
          "accessLocation  as  location_code",
          "linkValue as link_value",
          "accessLocation as  sid ",
          "locationIndex  as  location_index",
          "linkType as link_type",
          "adPuttingId as ad_putting_id",
          "positionType as position_type",
          "accountId as account_id",
          "dataSource as data_source",
          "recommendType as recommend_type",
          "datetime as date_time",
          "'one' as flag"
        )
      ).unionAll(
        getChannelInfo.selectExpr(
          "romVersion as rom_version",
          "firmwareVersion as firmware_version",
          "productLine as product_line",
          "productSN as product_sn",
          "contentType as page",
          "accesssArea as area_name",
          "accessLocation as  location_code",
          "linkValue as link_value",
          "accessLocation as  sid ",
          "locationIndex as location_index",
          "linkType as link_type",
          "adPuttingId as ad_putting_id",
          "positionType as position_type",
          "accountId as account_id",
          "dataSource as data_source",
          "recommendType as recommend_type",
          "datetime as date_time",
          "'one' as flag"
        )
      ).unionAll(getVod20LauncherInfo.selectExpr(
        "romVersion as rom_version",
        "firmwareVersion as firmware_version",
        "productLine as product_line",
        "productSN as product_sn",
        "'home' as page",
        "tableCode as area_name",
        "elementCode as  location_code",
        "linkValue as link_value",
        "linkValue as  sid ",
        "positionIndex as location_index",
        "linkType as link_type",
        "adPuttingId as ad_putting_id",
        "positionType as position_type",
        "accountId as account_id",
        "dataSource as data_source",
        "recommendType as recommend_type",
        "datetime as date_time",
        "'two' as flag"
      )).unionAll(getMovieInfo.selectExpr(
        "romVersion as rom_version",
        "firmwareVersion as firmware_version",
        "productLine as product_line",
        "productSN as product_sn",
        "contentType as page",
        "trim(accesssArea) as area_name",
        "accessLocation as  location_code",
        "linkValue as link_value",
        "accessLocation as  sid ",
        "locationIndex as location_index",
        "linkType as link_type",
        "adPuttingId as ad_putting_id",
        "positionType as position_type",
        "accountId as account_id",
        "dataSource as data_source",
        "recommendType as recommend_type",
        "datetime as date_time",
        "'one' as flag"
      )
      ).unionAll(
        getWuiButton.selectExpr(
          "romVersion as rom_version",
          "firmwareVersion as firmware_version",
          "productLine as product_line",
          "productSN as product_sn",
          "pageType as page",
          "buttonType as area_name",
          "elementCode as  location_code",
          "linkValue as link_value",
          "linkValue as  sid ",
          "positionIndex as location_index",
          "linkType as link_type",
          "adPuttingId as ad_putting_id",
          "positionType as position_type",
          "accountId as account_id",
          "dataSource as data_source",
          "recommendType as recommend_type",
          "datetime as date_time",
          "'one' as flag"
        )
      )


    }






  def addColumn(df: DataFrame, fields: List[(String, Any, DataType)]): DataFrame = {
    var dataFrame: DataFrame = df
    fields.foreach(tuple => {
      val field = tuple._1
      val value = tuple._2
      val dataType = tuple._3
      val flag = dataFrame.schema.fieldNames.contains(field)
      if (!flag) {
        dataFrame = dataFrame.withColumn(field, lit(value).cast(dataType))
      }
    })
    dataFrame
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


}
