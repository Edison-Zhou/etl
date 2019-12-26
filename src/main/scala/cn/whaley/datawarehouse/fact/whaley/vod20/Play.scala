package cn.whaley.datawarehouse.fact.whaley.vod20

import java.io.File

import cn.whaley.bigdata.dw.path.entity.DestinationInfo
import cn.whaley.bigdata.dw.path.global.PathNames
import cn.whaley.bigdata.dw.path.util.ParseUtil
import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition, UserDefinedColumn}
import cn.whaley.datawarehouse.fact.FactEtlBase
import cn.whaley.datawarehouse.fact.constant.Constants.FACT_HDFS_BASE_PATH_TMP
import cn.whaley.datawarehouse.fact.whaley.util._
import cn.whaley.datawarehouse.util.{DataExtractUtils, DateFormatUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

/**
 * Created by Tony on 18/4/18.
 */
object Play extends FactEtlBase {

  topicName = "fact_whaley_play"

  source = "vod20"

  partition = 300

  addColumns = List(

    //首页版本
    UserDefinedColumn("udc_wui_version", udf(LauncherEntranceUtils.wuiVersionFromPlay: (String, String) => String),
      List("romVersion", "firmwareVersion")),

    //首页维度
    UserDefinedColumn("udc_launcher_access_area",
      udf(LauncherEntranceUtils.launcherAccessAreaFromPlayVod: (String,String) => String),
      List("path_launcher_click_elementcode","udc_version_flag")),
    UserDefinedColumn("udc_launcher_access_location",
      udf(LauncherEntranceUtils.launcherAccessLocationFromPlayVod: (String, String,String) => String),
      List("path_launcher_click_elementcode", "path_launcher_click_linkvalue","udc_version_flag")),
    UserDefinedColumn("udc_launcher_location_index",
      udf(LauncherEntranceUtils.launcherLocationIndexFromPlayVod: (String, String, String,String) => Int),
      List("path_launcher_click_tablecode",
        "path_launcher_click_elementcode", "path_launcher_click_positionindex","udc_version_flag")),

    //频道首页维度(含详情页)
    UserDefinedColumn("udc_page_code",
      udf(ChannelLauncherEntranceUtils.channelhomePageCodeFromPlayVod: (String, String, String) => String),
      List("path_function_sitetree_click_contenttype", "path_sitetree_click_contenttype",
        "path_channelhome_click_contenttype"
      )),

    UserDefinedColumn("udc_page_area_code",
      udf(ChannelLauncherEntranceUtils.channelhomeAreaCodeFromPlayVod: (String, String, String, String, String, String) => String),
      List("path_function_sitetree_click_contenttype", "path_sitetree_click_contenttype",
        "path_function_sitetree_click_stationcode", "path_sitetree_click_sitetreecode",
        "path_channelhome_click_contenttype", "path_channelhome_click_accessarea"
      )),

    UserDefinedColumn("udc_page_location_code",
      udf(ChannelLauncherEntranceUtils.channelhomeLocationCodeFromPlayVod: (String, String, String, String) => String),
      List("path_channelhome_click_contenttype", "path_channelhome_click_accessarea",
        "path_channelhome_click_linkvalue", "path_columncenter_click_elementcode"
      )),

    UserDefinedColumn("udc_page_location_index",
      udf(ChannelLauncherEntranceUtils.channelhomeLocationIndexFromPlayVod: (String, String, String, String) => Int),
      List("path_channelhome_click_contenttype", "path_channelhome_click_accessarea",
        "path_channelhome_click_linkvalue", "path_channelhome_click_locationindex")),


    //站点树维度
    UserDefinedColumn("udc_last_category",
      udf(ListCategoryUtils.getLastFirstCodeVod: (String, String, String, String, String, String) => String),
      List("path_function_sitetree_click_contenttype", "path_function_sitetree_click_stationcode",
        "path_sitetree_click_contenttype", "path_sitetree_click_sitetreecode",
        "path_function_sitetree_click_pagevalue", "path_sitetree_click_pagevalue")),
    UserDefinedColumn("udc_last_second_category",
      udf(ListCategoryUtils.getLastSecondCodeVod: (String, String, String, String) => String),
      List("path_function_sitetree_click_contenttype", "path_sitetree_click_contenttype",
        "path_function_sitetree_click_pagevalue", "path_sitetree_click_pagevalue")),

    //路径contenttype
    UserDefinedColumn("udc_path_content_type",
      udf(ContentTypeUtils.getPathContenttypeFromPlayVod: (String, String, String) => String),
      List("path_channelhome_click_contenttype", "path_sitetree_click_contenttype",
        "path_function_sitetree_click_contenttype")),


    // 根据节目类型分解videosid

    UserDefinedColumn("video_sid",
      udf(VideoTypeUtils.getVideoSidFromPlayVod: (String, String) => String),
      List("videoSid", "linkType")),
    UserDefinedColumn("episode_sid",
      udf(VideoTypeUtils.getEpisodeSidFromPlayVod: (String, String) => String),
      List("episodeSid", "linkType")),
    UserDefinedColumn("omnibus_sid",
      udf(VideoTypeUtils.getOmnibusSidFromPlayVod: (String, String) => String),
      List("videoSid", "linkType")),
    UserDefinedColumn("udc_mv_hot_key",
      udf(VideoTypeUtils.getRankSidFromPlayVod: (String, String) => String),
      List("videoSid", "linkType")),
    UserDefinedColumn("match_sid",
      udf(VideoTypeUtils.getMatchSidFromPlayVod: (String, String) => String),
      List("videoSid", "linkType")),
    UserDefinedColumn("udc_radio_sid",
      udf(VideoTypeUtils.getRadioSidFromPlayVod: (String, String) => String),
      List("videoSid", "linkType")),
    UserDefinedColumn("udc_singer_sid",
      udf(VideoTypeUtils.getSingerSidFromPlayVod: (String, String) => String),
      List("videoSid", "linkType")),

    //last algorithmtype
    UserDefinedColumn("last_alg_type",
      udf(LastInforUtils.getLastAlgorithmTypeFromPlayVod: (String, String, String, String, String, String) => String),
      List("path_playexit_click_algorithmtype","path_detail_click_algorithmtype",
        "path_filter_algorithmtype","path_sitetree_click_algorithmtype",
        "path_channelhome_click_algorithmtype","path_launcher_click_algorithmtype")),

    UserDefinedColumn("last_alg",
      udf(LastInforUtils.getLastAlgFromPlayVod: (String, String, String, String, String, String) => String),
      List("path_playexit_click_algorithmtype","path_detail_click_algorithmtype",
        "path_filter_algorithmtype","path_sitetree_click_algorithmtype",
        "path_channelhome_click_algorithmtype","path_launcher_click_algorithmtype")),

    //last datasource
    UserDefinedColumn("last_datasource",
      udf(LastInforUtils.getLastDatasource: (String, String) => String),
      List("path_channelhome_click_datasource", "path_launcher_click_datasource")),


    //search
    UserDefinedColumn("udc_search_from_hot_word",
      udf(SearchUtils.isHotSearchWordFromPlayVod: String => Int),
      List("path_searchresult_allsearchvalue")),

    UserDefinedColumn("udc_search_result_index",
      udf(SearchUtils.getSearchResultIndexFromPlayVod: (String, String) => Int),
      List("path_searchresult_allsearchindex", "path_searchresult_resultindex")),

    UserDefinedColumn("udc_search_from_associational_word",
      udf(SearchUtils.isAssociationalSearchWordFromPlayVod: String => Int),
      List("path_searchresult_associationword")),

    UserDefinedColumn("udc_search_from",
      udf(SearchUtils.getSearchFromVod: (String, String) => String),
      List("path_searchresult_linkvalue", "path_function_sitetree_click_contenttype")),


    //filter
    UserDefinedColumn("retrieval",
      udf(RetrievalUtils.getFilterConditionFromPlayVod: (String, String) => String),
      List("path_filter_sorttype", "path_filter_conditionlist")),


    //语音搜索标识
    UserDefinedColumn("voice_search",
      udf(SearchUtils.isVoiceSearchFromPlayVod: (String) => String),
      List("path_voiceuse_parsedcontent")),

    //详情页点击区域

    UserDefinedColumn("udc_detail_location_index",
      udf(ChannelLauncherEntranceUtils.detailLocationIndexFromPlayVod: (String) => Int),
      List("path_detail_click_locationindex")),



    //播放来源(含专题/详情页/退出推荐/标签/明星)
    UserDefinedColumn("play_source",
      udf(PlaySourceUtils.getPlaySourceFromPlayVod: (String, String, String, String, String) => String),
      List("path_subject_click_linkvalue", "path_tag_click_linkvalue",
        "path_star_action_linkvalue", "path_playexit_click_linkvalue", "path_detail_click_linkvalue")),


    UserDefinedColumn("dim_date", udf(getDimDate: String => String), List("datetime")),
    UserDefinedColumn("dim_time", udf(getDimTime: String => String), List("datetime"))

  )

  //  columnsFromSource = (ParseUtil.dimColumnNames ++ DimensionKeyList.PATH_PLAY_KEY_LIST).map(c => (c, c))

  columnsFromSource = List(
    ("firmware_version", "firmwareVersion"),
    ("rom_version", "romVersion"),
    ("wui_version", "udc_wui_version"),

//    ("launcher_access_area", "udc_launcher_access_area"),
//    ("launcher_access_location", "udc_launcher_access_location"),
//    ("launcher_location_index", "udc_launcher_location_index"),
//    ("page_code", "udc_page_code"),
//    ("page_area_code", "udc_page_area_code"),
//    ("page_location_code", "udc_page_location_code"),
//    ("page_location_index", "udc_page_location_index"),
//    ("last_category", "udc_last_category"),
//    ("last_second_category", "udc_last_second_category"),

    ("last_alg_type","last_alg_type"),
    ("last_alg","last_alg"),
//    ("retrieval","retrieval"),
    ("duration", "duration"),
    ("product_sn", "productSN"),
    ("user_id", "userId"),
    ("network_type","networkType"),
    ("vod_version", "vodVersion"),
    ("trailer_process", "trailerProcess"),
    ("trailer_mark", "trailerMark"),
    ("trailer_duration", "cast(trailerDuration as bigint)"),
    ("preload_mark", "preloadMark"),
    ("program_resolution", "resolution"),
    ("program_duration", "cast(videoDuration as bigint)"),
    ("display_id", "displayId"),
//    ("video_sid","videoSid"),
//    ("video_name","videoName"),
    ("videosid_level", "vipLevel"),
    ("episode_level", "episodeVipLevel"),
    ("play_id", "playId"),
    ("play_linktype", "linkType"),
    ("video_source", "playSource"),
    ("log_session_id", "logSessionId"),
    ("current_vip_level", "currentVipLevel"),
    ("current_resolution","currentResolution"),
    ("product_line", "case when productLine is null or trim(productLine) = '' then 'helios' else productLine end"),
    ("content_type", "case when udc_path_content_type is not null and trim(udc_path_content_type) = 'interest' then udc_path_content_type " +
      "else dim_whaley_program.content_type end"),
    ("play_content_type",
      "case when dim_whaley_subject.subject_content_type is not null then dim_whaley_subject.subject_content_type " +
        "when udc_path_content_type is not null and trim(udc_path_content_type) = 'interest' then udc_path_content_type " +
        "when dim_whaley_program.content_type is not null then dim_whaley_program.content_type " +
        "when trim(contentType) = '' then null else contentType end"),
    ("subject_model", " path_subject_click_subjectmodel"),
    ("subject_is_item", "path_subject_click_isitem"),
    ("play_source","play_source"),

    ("channelhome_click_locataionword", "path_channelhome_click_locationword"),

    ("vipclub_recommend_date", "path_vip_dailyrecommend_click_recommenddate"),

    ("columncenter_code", "path_columncenter_click_elementcode"),
    ("columncenter_location_index","path_columncenter_click_locationindex"),

    ("recenttask_type", "path_recenttask_clicktype"),

    ("playexit_pre_sid", "path_playexit_click_videosid"),
    ("playexit_index", "path_playexit_click_locationindex"),

    ("tag_name", "path_tag_click_tagname"),
    ("tag_location_index","path_tag_click_locationindex"),

    ("preparse_voice", "path_voiceuse_preparsecontent"),
    ("parse_voice", "path_voiceuse_parsedcontent"),
    ("voice_dialect", "path_voiceuse_dialecttype"),
    ("voice_type", "path_voiceuse_voicetype"),
    ("voice_search","voice_search"),
    ("voice_pre_search_tab","path_voice_searchresult_click_pre_searchtab"),
    ("voice_res_search_tab","path_voice_searchresult_click_res_searchtab"),
    ("voice_pre_location_index","path_voice_searchresult_click_pre_locationindex"),
    ("voice_res_location_index","path_voice_searchresult_click_res_locationindex"),
    ("voice_pre_result_type","path_voice_searchresult_click_pre_resulttype"),
    ("voice_res_result_type","path_voice_searchresult_click_res_resulttype"),

    ("search_keyword", "path_searchresult_searchtext"),
    ("search_rec_keyword", "case when path_searchresult_allsearchvalue is null or " +
      "trim(path_searchresult_allsearchvalue) = '' " +
      "or path_searchresult_allsearchvalue = 'none' " +
      "then path_searchresult_associationword " +
      "else path_searchresult_allsearchvalue end"),
    ("launcher_link_type","path_launcher_click_linktype"),
    ("launcher_link_value","path_launcher_click_linkvalue"),
    ("detail_click_kids_version","path_detail_click_kidsversion"),
    ("filter_location_index","path_filter_locationindex"),
    ("poster_location_index","path_poster_click_locationindex"),
    ("poster_name","path_poster_click_postername"),




    ("dim_date", "dim_date"),
    ("dim_time", "dim_time")

  )

  dimensionsNeedInFact = List("dim_whaley_program", "dim_whaley_subject")

  dimensionColumns = List(
    //用户
    new DimensionColumn("dim_whaley_product_sn",
      List(DimensionJoinCondition(Map("productSN" -> "product_sn"))),
      List(("product_sn_sk", "product_sn_sk"), ("web_location_sk", "user_web_location_sk"))),

    //账号
    new DimensionColumn("dim_whaley_account",
      List(DimensionJoinCondition(Map("accountId" -> "account_id"))), "account_sk"),

    //首页入口
    new DimensionColumn("dim_whaley_launcher_entrance",
      List(DimensionJoinCondition(
        Map("udc_wui_version" -> "launcher_version",
          "udc_launcher_access_area" -> "access_area_code",
          "udc_launcher_access_location" -> "access_location_code",
          "udc_launcher_location_index" -> "launcher_location_index")),
        DimensionJoinCondition(
          Map("udc_wui_version" -> "launcher_version",
            "udc_launcher_access_area" -> "access_area_code",
            "udc_launcher_access_location" -> "access_location_code"),
          "launcher_location_index = -1")
      ), "launcher_entrance_sk"),

    //频道首页入口
    new DimensionColumn("dim_whaley_page_entrance",
      List(DimensionJoinCondition(
        Map("udc_page_code" -> "page_code", "udc_page_area_code" -> "area_code",
          "udc_page_location_code" -> "location_code", "udc_page_location_index" -> "location_index")
      ),
        DimensionJoinCondition(
          Map("udc_page_code" -> "page_code", "udc_page_area_code" -> "area_code",
            "udc_page_location_index" -> "location_index"))
      ), "page_entrance_sk"),

    //站点树
    new DimensionColumn("dim_whaley_source_site",
      List(DimensionJoinCondition(
        Map("udc_last_category" -> "last_first_code", "udc_last_second_category" -> "last_second_code")
      )), "source_site_sk"),

    //搜索
    new DimensionColumn("dim_whaley_search",
      List(DimensionJoinCondition(
        Map("path_searchresult_searchtab" -> "search_tab", "udc_search_from" -> "search_from",
          "udc_search_from_hot_word" -> "search_from_hot_word",
          "udc_search_from_associational_word" -> "search_from_associational_word",
          "udc_search_result_index" -> "search_result_index")
      ),
        DimensionJoinCondition(
          Map("path_searchresult_searchtab" -> "search_tab",
            "udc_search_from_hot_word" -> "search_from_hot_word",
            "udc_search_from_associational_word" -> "search_from_associational_word",
            "udc_search_result_index" -> "search_result_index"),
          "search_from = 'unknown'", null, "udc_search_from is not null"
        )), "search_sk"),


    //筛选
    new DimensionColumn("dim_whaley_retrieval",
      List(DimensionJoinCondition(
        Map("retrieval" -> "retrieval_key", "path_filter_contenttype" -> "content_type")
      )), "retrieval_sk"),

    //剧头
    new DimensionColumn("dim_whaley_program",
      List(DimensionJoinCondition(Map("video_sid" -> "sid"))), "program_sk"),

    //剧集
    new DimensionColumn("dim_whaley_program", "dim_whaley_program_episode",
      List(DimensionJoinCondition(Map("episode_sid" -> "sid"))), "program_sk", "episode_program_sk"),

    //专题
    new DimensionColumn("dim_whaley_subject",
      List(DimensionJoinCondition(Map("path_subject_click_subjectcode" -> "subject_code"))), "subject_sk"),

    //体育比赛
    new DimensionColumn("dim_whaley_sports_match",
      List(DimensionJoinCondition(Map("match_sid" -> "match_sid"))), "match_sk"),

    //音乐精选集
    new DimensionColumn("dim_whaley_mv_topic",
      List(DimensionJoinCondition(Map("omnibus_sid" -> "mv_topic_sid"))), "mv_topic_sk"),

    //歌手
    new DimensionColumn("dim_whaley_singer",
      List(DimensionJoinCondition(Map("udc_singer_sid" -> "singer_sid"))), "singer_sk"),

    //音乐电台
    new DimensionColumn("dim_whaley_radio",
      List(DimensionJoinCondition(Map("udc_radio_sid" -> "radio_sid"))), "radio_sk"),

    //音乐榜单
    new DimensionColumn("dim_whaley_mv_hot_list",
      List(DimensionJoinCondition(Map("udc_mv_hot_key" -> "mv_hot_rank_id"))), "mv_hot_sk"),

    //明星
    new DimensionColumn("dim_whaley_cast",
      List(DimensionJoinCondition(Map("path_star_action_starid" -> "cast_sid"))), "cast_sk"),

    //详情页点击区域
    new DimensionColumn("dim_whaley_page_entrance",
      List(
        DimensionJoinCondition(
          Map("path_detail_click_elementcode" -> "area_code",
            "udc_detail_location_index" -> "location_index"),
          "page_code = 'detail'")
      ), "page_entrance_sk","detail_click_entrance_sk"),


    //节目聚合维度
    new DimensionColumn("dim_whaley_link_type",
      List(
        DimensionJoinCondition(Map("videoSid" -> "link_value_code", "linkType" -> "link_type_code")),
        DimensionJoinCondition(Map("path_subject_click_subjectcode" -> "link_value_code"),
          "link_type_code = 4")
      ), "link_type_sk"),

    //路径聚合维度
    new DimensionColumn("dim_whaley_area_source_agg",
      List(
        DimensionJoinCondition(Map(), "source_code = 'voice_search'", null, "voice_search = 'true'"),
        DimensionJoinCondition(
          Map("udc_last_category" -> "sub_module_code", "udc_last_second_category" -> "module_code"),
          "source_code = 'source_site'"),
        DimensionJoinCondition(//关联源数据中只包含一层站点树的情况
          Map("udc_last_second_category" -> "sub_module_code"),
          "source_code = 'source_site'", null, "udc_last_category is null"
        ),
        DimensionJoinCondition(Map("udc_page_code" -> "module_code", "udc_page_area_code" -> "sub_module_code"),
          "source_code = 'channel_entrance'"),
        DimensionJoinCondition(Map("udc_launcher_access_location" -> "sub_module_code"),
          "source_code = 'launcher_entrance'")
      ), "area_source_agg_sk")

  )

  factTime = null

  override def readSource(sourceDate: String, sourceHour: String): DataFrame = {
    val INPUT_PATH = FACT_HDFS_BASE_PATH_TMP + File.separator + "whaley_path_parse" + File.separator
    val path = INPUT_PATH + "path_play" + File.separator + sourceDate + File.separator + (if(sourceHour == null ) "*" else sourceHour)
    val pathResultDf = DataExtractUtils.readFromParquet(sqlContext, path)
    val rdd = pathResultDf.rdd.map(s => {
      implicit val formats = DefaultFormats
      val destination = read[DestinationInfo](s.getAs[String]("destination"))
      val valueMap = ParseUtil.parseDimension(destination)
      val playInfoMap = destination.dimInfoMap(PathNames.PATH_PLAY).dimMap
      valueMap ++ playInfoMap
    })
    val columnMap = rdd.first()
    var schema = new StructType()
    columnMap.keys.foreach(s => {
      schema = schema.add(StructField(s, StringType))
    })
    val rowRdd = rdd.map(d => Row.fromSeq(d.values.toSeq))
    val df = sqlContext.createDataFrame(rowRdd, schema)
              .withColumn("udc_version_flag", lit("two")) //表示vod20
    if(sourceHour == null) {
      //在按天执行的情况下，通过关联停播日志获取duration字段
      val playEndDf =
        DataExtractUtils.readFromOds(sqlContext, "ods_view.log_whaleytv_main_path_play", sourceDate, null)
        .where("event in ('userexit', 'selfend', 'errorexit')").select("playId", "duration")
        .union(
          DataExtractUtils.readFromOds(sqlContext, "ods_view.log_whaleytv_main_path_play", DateFormatUtils.enDateAdd(sourceDate, 1), null)
            .where("event in ('userexit', 'selfend', 'errorexit')").select("playId", "duration")
        )
      df.as("a").join(playEndDf.as("b"), df("playId").equalTo(playEndDf("playId")), "leftouter")
        .selectExpr("a.*", "b.duration as endDuration").withColumn("duration", col("endDuration"))
    } else {
      df
    }
    //    sqlContext.createDataFrame(rdd.map(value => {
    //      value.values.toList
    //    })).toDF(columnMap.keys.toSeq: _*)
  }

  //  override def transform(params: Params, sourceDf: DataFrame): DataFrame = {
  //    //    sourceDf.printSchema()
  //    //    sourceDf.show(20, false)
  //    sourceDf
  //  }

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
