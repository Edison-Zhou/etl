package cn.whaley.datawarehouse.fact.whaley

import java.text.SimpleDateFormat

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition, UserDefinedColumn}
import cn.whaley.datawarehouse.fact.FactEtlBase
import cn.whaley.datawarehouse.fact.constant.LogPath
import cn.whaley.datawarehouse.fact.whaley.util.RomVersionUtils
import cn.whaley.datawarehouse.util.DataExtractUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}

/**
  * 创建人：huanghu
  * 创建时间：2017/6/28
  * 程序作用：对播放质量相关的日志进行合并建立事实表
  * 数据输入：播放质量的五个日志
  * 数据输出：播放质量事实表
  */
object PlayQuality extends FactEtlBase {
  topicName = "fact_whaley_play_quality"

  source = "default"

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
    ("app_pack_age", "app_pack_age"),
    ("app_version", "app_version"),
    ("sdk_version", "sdk_version"),
    ("rom_version", "rom_version"),
    ("firmware_version", "firmware_version"),
    ("net_type", "net_type"),
    ("play_type", "play_type"),
    ("preload_mark", "preload_mark"),
    ("current_vip_level", "current_vip_level"),
    ("source", "source"),
    ("cause", "cause "),
    ("cause_time", "cause_time"),
    ("user_operation", " user_operation"),
    ("auto_operation", "auto_operation"),
    ("retry_times", "retry_times"),
    ("play_url", "play_url"),
    ("result", "result"),
    ("error_code", "error_code "),
    ("definition", "definition"),
    ("start_time", "cast(udf_start_time as timestamp)"),
    ("end_time", "cast(udf_end_time as timestamp)"),
    ("duration", "cast(duration as double) "),
    ("play_session_id", "play_session_id"),
    ("start_play_session_id", "start_play_session_id"),
    ("trailer_process", "trailer_process"),
    ("product_line", "product_line "),
    ("user_id", "user_id "),
    ("dim_date", " dim_date"),
    ("dim_time", "dim_time")
  )
  dimensionColumns = List(
    /** 获得访问ip对应的地域维度user_web_location_sk */
    new DimensionColumn("dim_web_location",
      List(DimensionJoinCondition(Map("real_ip" -> "web_location_key"))),
      "web_location_sk","access_web_location_sk"),

    new DimensionColumn("dim_whaley_product_sn",
      List(DimensionJoinCondition(Map("product_sn" -> "product_sn"))),
      List(("product_sn_sk", "product_sn_sk"), ("web_location_sk", "user_web_location_sk"))),

    //剧头
    new DimensionColumn("dim_whaley_program",
      List(DimensionJoinCondition(Map("video_sid" -> "sid"))), "program_sk"),

    //剧集
    new DimensionColumn("dim_whaley_program", "dim_whaley_program_episode",
      List(DimensionJoinCondition(Map("episode_sid" -> "sid"))), "program_sk", "episode_program_sk"),

    //账号
    new DimensionColumn("dim_whaley_account",
      List(DimensionJoinCondition(Map("account_id" -> "account_id"))), "account_sk")

  )

  override def readSource(startDate: String, startHour: String): DataFrame = {
    sqlContext.udf.register("getVideoSid", getVideoSid _)
    sqlContext.udf.register("getEpisodeSid", getEpisodeSid _)

    val fields = List(
      ("exitType", null, StringType),
      ("videoTime", 0, IntegerType),
      ("bufferType", null, StringType),
      ("startPlaySessionId", null, StringType),
      ("errorCode", null, StringType),
      ("definition", null, StringType),
      ("result", null, StringType),
      ("userSwitch", 3, IntegerType),
      ("autoSwitch", 3, IntegerType),
      ("retryTimes", -1, IntegerType),
      ("playUrl", null, StringType),
      ("trailerProcess", null, StringType),
      ("preloadMark", null, StringType),
      ("currentVipLevel", null, StringType),
      ("phrase", null, StringType),
      ("datetime", null, StringType)
    )

    //    var getVideoDf = DataExtractUtils.readFromParquet(sqlContext, LogPath.HELIOS_GET_VIDEO, startDate)
    //    var startPlayDf = DataExtractUtils.readFromParquet(sqlContext, LogPath.HELIOS_START_PLAY, startDate)
    //    var parseDf = DataExtractUtils.readFromParquet(sqlContext, LogPath.HELIOS_PARSE, startDate)
    //    var bufferDf = DataExtractUtils.readFromParquet(sqlContext, LogPath.HELIOS_BUFFER, startDate)
    //    var endPlayDf = DataExtractUtils.readFromParquet(sqlContext, LogPath.HELIOS_END_PLAY, startDate)

    var getVideoDf = DataExtractUtils.readFromOds(sqlContext, "ods_view.log_whaleytv_main_helios_player_sdk_getvideoinfo", startDate, startHour)
    var startPlayDf = DataExtractUtils.readFromOds(sqlContext, "ods_view.log_whaleytv_main_helios_player_sdk_startplay", startDate, startHour)
    var parseDf = DataExtractUtils.readFromOds(sqlContext, "ods_view.log_whaleytv_main_helios_player_sdk_inner_outer_auth_parse", startDate, startHour)
    var bufferDf = DataExtractUtils.readFromOds(sqlContext, "ods_view.log_whaleytv_main_helios_player_sdk_buffer", startDate, startHour)
    var endPlayDf = DataExtractUtils.readFromOds(sqlContext, "ods_view.log_whaleytv_main_helios_player_sdk_endplay", startDate, startHour)

    getVideoDf = addColumn(getVideoDf, fields)
    startPlayDf = addColumn(startPlayDf, fields)
    parseDf = addColumn(parseDf, fields)
    bufferDf = addColumn(bufferDf, fields)
    endPlayDf = addColumn(endPlayDf, fields)

    //日志合并
    getVideoDf.selectExpr(
      "appPackage as app_pack_age",
      "appVersion as  app_version",
      "sdkVersion as sdk_version",
      "romVersion as rom_version",
      "firmwareVersion as firmware_version",
      "netType as net_type",
      "playType as play_type",
      "preloadMark as preload_mark",
      "currentVipLevel as  current_vip_level",
      "sourceList as source",
      "exitType as cause",
      "videoTime as cause_time",
      "(case  when userSwitch = 0 then 'false'  when userSwitch = 1 then 'true' else null end)  as user_operation",
      "(case  when autoSwitch = 0 then 'false'  when autoSwitch = 1 then 'true' else null end)  as  auto_operation",
      "retryTimes as retry_times",
      "playUrl as play_url",
      "(case when result = 'fail' then 'failure' else result end) as result",
      "errorCode as error_code",
      "definition as definition",
      "startTime as start_time",
      "endTime as end_time",
      "(endTime-startTime)*1.0/1000 as duration",
      "playSessionId as play_session_id",
      "startPlaySessionId as start_play_session_id",
      "trailerProcess as trailer_process",
      "productLine as product_line",
      "userId as user_id",
      "getVideoSid(videoSid,episodeSid,romVersion,firmwareVersion) as video_sid",
      "getEpisodeSid (videoSid,episodeSid,romVersion,firmwareVersion) as episode_sid",
      "datetime as date_time",
      "productSN as product_sn",
      "eventId as event_id",
      "phrase as phrase",
      "realIP as real_ip",
      "accountId as account_id"
    ).unionAll(
      startPlayDf.selectExpr(
        "appPackage as app_pack_age",
        "appVersion as  app_version",
        "sdkVersion as sdk_version",
        "romVersion as rom_version",
        "firmwareVersion as firmware_version",
        "netType as net_type",
        "playType as play_type",
        "preloadMark as preload_mark",
        "currentVipLevel as  current_vip_level",
        "source as source",
        "exitType as cause",
        "videoTime as cause_time",
        "(case  when userSwitch = 0 then 'false'  when userSwitch = 1 then 'true' else null end)  as user_operation",
        "(case  when autoSwitch = 0 then 'false'  when autoSwitch = 1 then 'true' else null end)  as  auto_operation",
        "retryTimes as retry_times",
        "playUrl as play_url",
        "(case when result = 'fail' then 'failure' else result end) as result",
        "errorCode as error_code",
        "definition as definition",
        "startTime as start_time",
        "endTime as end_time",
        "(endTime-startTime)*1.0/1000 as duration",
        "playSessionId as play_session_id",
        "startPlaySessionId as start_play_session_id",
        "trailerProcess as trailer_process",
        "productLine as product_line",
        "userId as user_id",
        "getVideoSid(videoSid,episodeSid,romVersion,firmwareVersion) as video_sid",
        "getEpisodeSid (videoSid,episodeSid,romVersion,firmwareVersion) as episode_sid",
        "datetime as date_time",
        "productSN as product_sn",
        "eventId as event_id",
        "phrase as phrase",
        "realIP as real_ip",
        "accountId as account_id"
      )
    ).unionAll(
      parseDf.selectExpr(
        "appPackage as app_pack_age",
        "appVersion as  app_version",
        "sdkVersion as sdk_version",
        "romVersion as rom_version",
        "firmwareVersion as firmware_version",
        "netType as net_type",
        "playType as play_type",
        "preloadMark as preload_mark",
        "currentVipLevel as  current_vip_level",
        "source as source",
        "exitType as cause",
        "videoTime as cause_time",
        "(case  when userSwitch = 0 then 'false'  when userSwitch = 1 then 'true' else null end)  as user_operation",
        "(case  when autoSwitch = 0 then 'false'  when autoSwitch = 1 then 'true' else null end)  as  auto_operation",
        "retryTimes as retry_times",
        "playUrl as play_url",
        "(case when result = 'fail' then 'failure' else result end) as result",
        "errorCode as error_code",
        "definition as definition",
        "startTime as start_time",
        "endTime as end_time",
        "(endTime-startTime)*1.0/1000 as duration",
        "playSessionId as play_session_id",
        "startPlaySessionId as start_play_session_id",
        "trailerProcess as trailer_process",
        "productLine as product_line",
        "userId as user_id",
        "getVideoSid(videoSid,episodeSid,romVersion,firmwareVersion) as video_sid",
        "getEpisodeSid (videoSid,episodeSid,romVersion,firmwareVersion) as episode_sid",
        "datetime as date_time",
        "productSN as product_sn",
        "eventId as event_id",
        "phrase as phrase",
        "realIP as real_ip",
        "accountId as account_id"
      )
    ).unionAll(
      bufferDf.selectExpr(
        "appPackage as app_pack_age",
        "appVersion as  app_version",
        "sdkVersion as sdk_version",
        "romVersion as rom_version",
        "firmwareVersion as firmware_version",
        "netType as net_type",
        "playType as play_type",
        "preloadMark as preload_mark",
        "currentVipLevel as  current_vip_level",
        "source as source",
        "bufferType as cause",
        "videoTime as cause_time",
        "(case  when userSwitch = 0 then 'false'  when userSwitch = 1 then 'true' else null end)  as user_operation",
        "(case  when autoSwitch = 0 then 'false'  when autoSwitch = 1 then 'true' else null end)  as  auto_operation",
        "retryTimes as retry_times",
        "playUrl as play_url",
        "(case when result = 'fail' then 'failure' else result end) as result",
        "errorCode as error_code",
        "definition as definition",
        "startTime as start_time",
        "endTime as end_time",
        "(endTime-startTime)*1.0/1000 as duration",
        "playSessionId as play_session_id",
        "startPlaySessionId as start_play_session_id",
        "trailerProcess as trailer_process",
        "productLine as product_line",
        "userId as user_id",
        "getVideoSid(videoSid,episodeSid,romVersion,firmwareVersion) as video_sid",
        "getEpisodeSid (videoSid,episodeSid,romVersion,firmwareVersion) as episode_sid",
        "datetime as date_time",
        "productSN as product_sn",
        "eventId as event_id",
        "phrase as phrase",
        "realIP as real_ip",
        "accountId as account_id"
      )
    ).unionAll(
      endPlayDf.selectExpr(
        "appPackage as app_pack_age",
        "appVersion as  app_version",
        "sdkVersion as sdk_version",
        "romVersion as rom_version",
        "firmwareVersion as firmware_version",
        "netType as net_type",
        "playType as play_type",
        "preloadMark as preload_mark",
        "currentVipLevel as  current_vip_level",
        "source as source",
        "exitType as cause",
        "videoTime as cause_time",
        "(case  when userSwitch = 0 then 'false'  when userSwitch = 1 then 'true' else null end)  as user_operation",
        "(case  when autoSwitch = 0 then 'false'  when autoSwitch = 1 then 'true' else null end)  as  auto_operation",
        "retryTimes as retry_times",
        "playUrl as play_url",
        "(case when result ='fail' then 'failure' else result end) as result",
        "errorCode as error_code",
        "definition as definition",
        "startTime as start_time",
        "endTime as end_time",
        "(endTime-startTime)*1.0/1000 as duration",
        "playSessionId as play_session_id",
        "startPlaySessionId as start_play_session_id",
        "trailerProcess as trailer_process",
        "productLine as product_line",
        "userId as user_id",
        "getVideoSid(videoSid,episodeSid,romVersion,firmwareVersion) as video_sid",
        "getEpisodeSid (videoSid,episodeSid,romVersion,firmwareVersion) as episode_sid",
        "datetime as date_time",
        "productSN as product_sn",
        "eventId as event_id",
        "phrase as phrase",
        "realIP as real_ip",
        "accountId as account_id"
      )
    )

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

  def getVideoSid(videoSid: String, episodeSid: String, romVersion: String, firmwareVersion: String): String = {
    try {
      val rom = RomVersionUtils.getRomVersion(romVersion, firmwareVersion)
      if (rom >= "02:00:00:00") videoSid
      else episodeSid
    } catch {
      case ex: Exception => ""
    }
  }

  def getEpisodeSid(videoSid: String, episodeSid: String, romVersion: String, firmwareVersion: String): String = {
    try {
      val rom = RomVersionUtils.getRomVersion(romVersion, firmwareVersion)
      if (rom >= "02:00:00:00") episodeSid
      else videoSid
    } catch {
      case ex: Exception => ""
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
