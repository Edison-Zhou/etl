package cn.whaley.datawarehouse.fact.moretv

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition, UserDefinedColumn}
import cn.whaley.datawarehouse.fact.FactEtlBase
import cn.whaley.datawarehouse.fact.constant.LogPath
import cn.whaley.datawarehouse.util.{DataExtractUtils, DateFormatUtils}
import org.apache.spark.sql.DataFrame
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType}
import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * 创建人：luoziyu
  * 创建时间：2017/7/4
  * 程序作用：对播放质量相关的日志进行合并建立事实表
  * 数据输入：播放质量的五个日志
  * 数据输出：播放质量事实表
  */
object PlayQuality extends FactEtlBase {
  topicName = "fact_medusa_play_quality"

  source = "main3x"

  addColumns = List(
    UserDefinedColumn("real_ip", udf(getIpKey: String => Long), List("real_ip")),
    UserDefinedColumn("dim_date", udf(getDimDate: String => String), List("date_time")),
    UserDefinedColumn("dim_time", udf(getDimTime: String => String), List("date_time")),
    UserDefinedColumn("event", udf(getEvent: (String, String) => String), List("event_id", "phrase")),
    UserDefinedColumn("udf_start_time", udf(getFormatTime: (Long) => String), List("start_time")),
    UserDefinedColumn("udf_end_time", udf(getFormatTime: (Long) => String), List("start_time"))
  )

  columnsFromSource = List(
    ("event", "event"),
    ("app_package", "app_package"),
    ("auto_version", "auto_version"),
    ("luascript", "luascript"),
    ("sdk_version", "sdk_version"),
    ("net_type", "net_type"),
    ("play_type", "play_type"),
    ("player_type", "player_type"),
    ("preload_mark", "preload_mark"),
    ("product_model", "product_model"),
    ("user_type", "user_type"),
    ("source", "source"),
    ("cause", "cause"),
    ("cause_time", "cause_time"),
    ("user_operation", "user_operation"),
    ("auto_operation", "auto_operation"),
    ("retry_times", "retry_times"),
    ("play_url", "play_url"),
    ("result", "result"),
    ("add_info", "add_info"),
    ("error_code", "error_code"),
    ("definition", "definition"),
    ("start_time", "cast(udf_start_time as timestamp)"),
    ("end_time", "cast(udf_end_time as timestamp)"),
    ("duration", "cast(duration as double)"),
    ("play_session_id", "play_session_id"),
    ("start_play_session_id", "start_play_session_id"),
    ("trailer_process", "trailer_process"),
    ("user_id", "user_id"),
    ("dim_date", "dim_date"),
    ("dim_time", "dim_time")
  )
  dimensionColumns = List(
    /** 获得访问ip对应的地域维度user_web_location_sk */
    new DimensionColumn("dim_web_location",
      List(DimensionJoinCondition(Map("real_ip" -> "web_location_key"))),
      "web_location_sk","access_web_location_sk"),

    /** 获得用户维度user_sk */
    new DimensionColumn("dim_medusa_terminal_user",
      List(DimensionJoinCondition(Map("user_id" -> "user_id"))),
      List(("user_sk", "user_sk"), ("web_location_sk", "user_web_location_sk"))),

    /** 获得用户登录维度user_login_sk */
//    new DimensionColumn("dim_medusa_terminal_user_login",
//      List(DimensionJoinCondition(Map("user_id" -> "user_id"))),
//      "user_login_sk"),

    /** 获得设备型号维度product_model_sk */
    new DimensionColumn("dim_medusa_product_model",
      List(DimensionJoinCondition(Map("product_model" -> "product_model"))),
      "product_model_sk"),

    /** 获得推广渠道维度promotion_sk */
    new DimensionColumn("dim_medusa_promotion",
      List(DimensionJoinCondition(Map("promotion_channel" -> "promotion_code"))),
      "promotion_sk"),

    /** 获得app版本维度app_version_sk */
    new DimensionColumn("dim_app_version",
      List(
        DimensionJoinCondition(Map("apk_series" -> "app_series", "apk_version" -> "version", "build_date" -> "build_time")),
        DimensionJoinCondition(Map("apk_series" -> "app_series", "apk_version" -> "version"))
      ),
      "app_version_sk"),

    /** 获得节目维度program_sk */
    new DimensionColumn("dim_medusa_program",
      List(DimensionJoinCondition(Map("video_sid" -> "sid"))),
      "program_sk"),

    /** 获得剧集节目维度episode_program_sk */
    new DimensionColumn("dim_medusa_program", "dim_medusa_program_episode",
      List(DimensionJoinCondition(Map("episode_sid" -> "sid"))),
      "program_sk", "episode_program_sk"),

    /** 获得账号维度account_sk */
    new DimensionColumn("dim_medusa_account",
      List(DimensionJoinCondition(Map("account_id" -> "account_id"))),
      "account_sk")
  )

  override def readSource(startDate: String, startHour: String): DataFrame = {
    val date = DateUtils.addDays(DateFormatUtils.readFormat.parse(startDate), 1)
    val realStartDate = DateFormatUtils.readFormat.format(date)

    val fields = List(
      ("exitType", null, StringType),
      ("videoTime", 0, IntegerType),
      ("bufferType", null, StringType),
      ("luascript", null, StringType),
      ("sourceArea", null, StringType),
      ("startPlaySessionId", null, StringType),
      ("errorCode", null, StringType),
      ("definition", null, StringType),
      ("result", null, StringType),
      ("autoVersion", null, StringType),
      ("userSwitch", 3, IntegerType),
      ("autoSwitch", 3, IntegerType),
      ("retryTimes", -1L, LongType),
      ("preloadMark", null, StringType),
      ("trailerProcess", null, StringType),
      ("playUrl", null, StringType),
      ("playerType", null, StringType),
      ("phrase", null, StringType),
      ("addInfo", null, StringType),
      ("userType", null, StringType)
    )

    //    //节目信息
    //    var getVideoDf = DataExtractUtils.readFromParquet(sqlContext, LogPath.MEDUSA_PLAY_VIDEO_INFO, realStartDate)
    //    //起播
    //    var startPlayDf = DataExtractUtils.readFromParquet(sqlContext, LogPath.MEDUSA_PLAY_START, realStartDate)
    //    //解析
    //    var parseDf = DataExtractUtils.readFromParquet(sqlContext, LogPath.MEDUSA_PLAY_PARSE, realStartDate)
    //    //缓冲
    //    var bufferDf = DataExtractUtils.readFromParquet(sqlContext, LogPath.MEDUSA_PLAY_BUFFER, realStartDate)
    //    //结束播放
    //    var endPlayDf = DataExtractUtils.readFromParquet(sqlContext, LogPath.MEDUSA_PLAY_END, realStartDate)

    //节目信息
    var getVideoDf = DataExtractUtils.readFromOds(sqlContext, "ods_view.log_medusa_main3x_medusa_player_sdk_getvideoinfo", startDate, startHour)
    //起播
    var startPlayDf = DataExtractUtils.readFromOds(sqlContext, "ods_view.log_medusa_main3x_medusa_player_sdk_startplay", startDate, startHour)
    //解析
    var parseDf = DataExtractUtils.readFromOds(sqlContext, "ods_view.log_medusa_main3x_medusa_player_sdk_inner_outer_auth_parse", startDate, startHour)
    //缓冲
    var bufferDf = DataExtractUtils.readFromOds(sqlContext, "ods_view.log_medusa_main3x_medusa_player_sdk_buffer", startDate, startHour)
    //结束播放
    var endPlayDf = DataExtractUtils.readFromOds(sqlContext, "ods_view.log_medusa_main3x_medusa_player_sdk_endplay", startDate, startHour)

    getVideoDf = addColumn(getVideoDf, fields)
    startPlayDf = addColumn(startPlayDf, fields)
    parseDf = addColumn(parseDf, fields)
    bufferDf = addColumn(bufferDf, fields)
    endPlayDf = addColumn(endPlayDf, fields)

    //日志合并
    getVideoDf.selectExpr(
      "productModel as product_model",
      "luascript as luascript",
      "appPackage as app_package",
      "appVersion as app_version",
      "sdkVersion as sdk_version",
      "autoVersion as auto_version",
      "netType as net_type",
      "playType as play_type",
      "playerType as player_type",
      "preloadMark as preload_mark",
      "userType as user_type",
      "sourceList as source",
      "addInfo as add_info",
      "promotionChannel as promotion_channel",
      "apkSeries as apk_series",
      "apkVersion as apk_version",
      "videoSid as episode_sid",
      "episodeSid as video_sid",
      "buildDate as build_date",
      "sourceArea as source_area",
      "exitType as cause",
      "videoTime as cause_time",
      "(case  when userSwitch = 0 then 'false'  when userSwitch = 1 then 'true' else null end)  as user_operation",
      "(case  when autoSwitch = 0 then 'false'  when autoSwitch = 1 then 'true' else null end)  as  auto_operation",
      "retryTimes as retry_times",
      "playUrl as play_url",
      "realIP as real_ip",
      "(case when result = 'fail' then 'failure' else result end) as result",
      "errorCode as error_code",
      "definition as definition",
      "startTime as start_time",
      "endTime as end_time",
      "(endTime-startTime)*1.0/1000 as duration",
      "playSessionId as play_session_id",
      "startPlaySessionId as start_play_session_id",
      "trailerProcess as trailer_process",
      "userId as user_id",
      "datetime as date_time",
      "eventId as event_id",
      "phrase as phrase",
      "accountId as account_id"
    ).unionAll(
      startPlayDf.selectExpr(
        "productModel as product_model",
        "luascript as luascript",
        "appPackage as app_package",
        "appVersion as app_version",
        "sdkVersion as sdk_version",
        "autoVersion as auto_version",
        "netType as net_type",
        "playType as play_type",
        "playerType as player_type",
        "preloadMark as preload_mark",
        "userType as user_type",
        "source as source",
        "addInfo as add_info",
        "promotionChannel as promotion_channel",
        "apkSeries as apk_series",
        "apkVersion as apk_version",
        "videoSid as episode_sid",
        "episodeSid as video_sid",
        "buildDate as build_date",
        "sourceArea as source_area",
        "exitType as cause",
        "videoTime as cause_time",
        "(case  when userSwitch = 0 then 'false'  when userSwitch = 1 then 'true' else null end)  as user_operation",
        "(case  when autoSwitch = 0 then 'false'  when autoSwitch = 1 then 'true' else null end)  as  auto_operation",
        "retryTimes as retry_times",
        "playUrl as play_url",
        "realIP as real_ip",
        "(case when result = 'fail' then 'failure' else result end) as result",
        "errorCode as error_code",
        "definition as definition",
        "startTime as start_time",
        "endTime as end_time",
        "(endTime-startTime)*1.0/1000 as duration",
        "playSessionId as play_session_id",
        "startPlaySessionId as start_play_session_id",
        "trailerProcess as trailer_process",
        "userId as user_id",
        "datetime as date_time",
        "eventId as event_id",
        "phrase as phrase",
        "accountId as account_id"
      )
    ).unionAll(
      parseDf.selectExpr(
        "productModel as product_model",
        "luascript as luascript",
        "appPackage as app_package",
        "appVersion as app_version",
        "sdkVersion as sdk_version",
        "autoVersion as auto_version",
        "netType as net_type",
        "playType as play_type",
        "playerType as player_type",
        "preloadMark as preload_mark",
        "userType as user_type",
        "source as source",
        "addInfo as add_info",
        "promotionChannel as promotion_channel",
        "apkSeries as apk_series",
        "apkVersion as apk_version",
        "videoSid as episode_sid",
        "episodeSid as video_sid",
        "buildDate as build_date",
        "sourceArea as source_area",
        "exitType as cause",
        "videoTime as cause_time",
        "(case  when userSwitch = 0 then 'false'  when userSwitch = 1 then 'true' else null end)  as user_operation",
        "(case  when autoSwitch = 0 then 'false'  when autoSwitch = 1 then 'true' else null end)  as  auto_operation",
        "retryTimes as retry_times",
        "playUrl as play_url",
        "realIP as real_ip",
        "(case when result = 'fail' then 'failure' else result end) as result",
        "errorCode as error_code",
        "definition as definition",
        "startTime as start_time",
        "endTime as end_time",
        "(endTime-startTime)*1.0/1000 as duration",
        "playSessionId as play_session_id",
        "startPlaySessionId as start_play_session_id",
        "trailerProcess as trailer_process",
        "userId as user_id",
        "datetime as date_time",
        "eventId as event_id",
        "phrase as phrase",
        "accountId as account_id"
      )
    ).unionAll(
      bufferDf.selectExpr(
        "productModel as product_model",
        "luascript as luascript",
        "appPackage as app_package",
        "appVersion as app_version",
        "sdkVersion as sdk_version",
        "autoVersion as auto_version",
        "netType as net_type",
        "playType as play_type",
        "playerType as player_type",
        "preloadMark as preload_mark",
        "userType as user_type",
        "source as source",
        "addInfo as add_info",
        "promotionChannel as promotion_channel",
        "apkSeries as apk_series",
        "apkVersion as apk_version",
        "videoSid as episode_sid",
        "episodeSid as video_sid",
        "buildDate as build_date",
        "sourceArea as source_area",
        "exitType as cause",
        "videoTime as cause_time",
        "(case  when userSwitch = 0 then 'false'  when userSwitch = 1 then 'true' else null end)  as user_operation",
        "(case  when autoSwitch = 0 then 'false'  when autoSwitch = 1 then 'true' else null end)  as  auto_operation",
        "retryTimes as retry_times",
        "playUrl as play_url",
        "realIP as real_ip",
        "(case when result = 'fail' then 'failure' else result end) as result",
        "errorCode as error_code",
        "definition as definition",
        "startTime as start_time",
        "endTime as end_time",
        "(endTime-startTime)*1.0/1000 as duration",
        "playSessionId as play_session_id",
        "startPlaySessionId as start_play_session_id",
        "trailerProcess as trailer_process",
        "userId as user_id",
        "datetime as date_time",
        "eventId as event_id",
        "phrase as phrase",
        "accountId as account_id"
      )
    ).unionAll(
      endPlayDf.selectExpr(
        "productModel as product_model",
        "luascript as luascript",
        "appPackage as app_package",
        "appVersion as app_version",
        "sdkVersion as sdk_version",
        "autoVersion as auto_version",
        "netType as net_type",
        "playType as play_type",
        "playerType as player_type",
        "preloadMark as preload_mark",
        "userType as user_type",
        "source as source",
        "addInfo as add_info",
        "promotionChannel as promotion_channel",
        "apkSeries as apk_series",
        "apkVersion as apk_version",
        "videoSid as episode_sid",
        "episodeSid as video_sid",
        "buildDate as build_date",
        "sourceArea as source_area",
        "exitType as cause",
        "videoTime as cause_time",
        "(case  when userSwitch = 0 then 'false'  when userSwitch = 1 then 'true' else null end)  as user_operation",
        "(case  when autoSwitch = 0 then 'false'  when autoSwitch = 1 then 'true' else null end)  as  auto_operation",
        "retryTimes as retry_times",
        "playUrl as play_url",
        "realIP as real_ip",
        "(case when result = 'fail' then 'failure' else result end) as result",
        "errorCode as error_code",
        "definition as definition",
        "startTime as start_time",
        "endTime as end_time",
        "(endTime-startTime)*1.0/1000 as duration",
        "playSessionId as play_session_id",
        "startPlaySessionId as start_play_session_id",
        "trailerProcess as trailer_process",
        "userId as user_id",
        "datetime as date_time",
        "eventId as event_id",
        "phrase as phrase",
        "accountId as account_id"
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

  //will use common util function class instead
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

  def getEvent(eventId: String, phrase: String): String = {
    try {
      if (phrase == null) {
        val event = eventId.split("-")
        val num = event.length
        event(num - 1)
      } else {
        phrase
      }
    } catch {
      case ex: Exception => ""
    }
  }


  def getFormatTime(time: Long): String = {
    try {
      val format =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      format.format(time)
    } catch {
      case ex: Exception => ""
    }
  }

}
