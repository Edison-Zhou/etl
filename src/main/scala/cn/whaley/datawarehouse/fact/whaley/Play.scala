package cn.whaley.datawarehouse.fact.whaley

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition, UserDefinedColumn}
import cn.whaley.datawarehouse.fact.FactEtlBase
import cn.whaley.datawarehouse.fact.constant.LogPath
import cn.whaley.datawarehouse.fact.whaley.util._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf

/**
  * Created by Tony on 17/5/16.
  */
object Play extends FactEtlBase {

  topicName = "fact_whaley_play"

  source = "vod10"

  odsTableName = "ods_view.log_whaleytv_main_play"

  partition = 300

  addColumns = List(
    UserDefinedColumn("udc_subject_code", udf(SubjectUtils.getSubjectCode: String => String), List("path")),
    UserDefinedColumn("udc_wui_version", udf(LauncherEntranceUtils.wuiVersionFromPlay: (String, String) => String),
      List("romVersion", "firmwareVersion")),
    UserDefinedColumn("udc_launcher_access_location",
      udf(LauncherEntranceUtils.launcherAccessLocationFromPath: (String, String) => String),
      List("path", "linkValue")),
    UserDefinedColumn("udc_launcher_location_index",
      udf(LauncherEntranceUtils.launcherLocationIndexFromPlay: (String, String) => Int),
      List("path","recommendLocation")),
    UserDefinedColumn("udc_launcher_access_area",
      udf(LauncherEntranceUtils.launcherAccessAreaFromPlayPath: String => String),
      List("path")),
    UserDefinedColumn("udc_recommend_position",
      udf(RecommendPositionUtils.getRecommendPosition: (String, String) => String),
      List("path", "pathSub")),
    UserDefinedColumn("udc_recommend_index",
      udf(RecommendPositionUtils.getRecommendIndex: (String, String) => Int),
      List("locationIndex", "udc_recommend_position")),
    UserDefinedColumn("udc_path_content_type",
      udf(ContentTypeUtils.getContentType: (String, String) => String),
      List("path", "contentType")),
    UserDefinedColumn("udc_page_code",
      udf(ChannelLauncherEntranceUtils.getPageEntrancePageCode: (String, String,String,String) => String),
      List("path", "contentType","romVersion","firmwareVersion")),
    UserDefinedColumn("udc_page_area_code",
      udf(ChannelLauncherEntranceUtils.getPageEntranceAreaCode: (String, String,String,String) => String),
      List("path", "contentType","romVersion","firmwareVersion")),
    UserDefinedColumn("udc_page_location_code",
      udf(ChannelLauncherEntranceUtils.getPageEntranceLocationCode: (String, String,String,String) => String),
      List("path", "contentType","romVersion","firmwareVersion")),
    UserDefinedColumn("udc_page_location_index",
      udf(ChannelLauncherEntranceUtils.getPageEntranceLocationIndex: (String, String,String,String) => Int),
      List("locationIndex", "contentType","romVersion","firmwareVersion")),
    UserDefinedColumn("udc_last_category",
      udf(ListCategoryUtils.getLastFirstCode: String => String),
      List("path")),
    UserDefinedColumn("udc_last_second_category",
      udf(ListCategoryUtils.getLastSecondCode: String => String),
      List("path")),
    UserDefinedColumn("udc_search_from",
      udf(SearchUtils.getSearchFrom: String => String),
      List("path")),
    UserDefinedColumn("udc_search_from_hot_word",
      udf(SearchUtils.isHotSearchWord: String => Int),
      List("hotSearchWord")),
    UserDefinedColumn("udc_search_from_associational_word",
      udf(SearchUtils.isAssociationalSearchWord: String => Int),
      List("searchAssociationalWord")),
    UserDefinedColumn("udc_search_result_index",
      udf(SearchUtils.getSearchResultIndex: String => Int),
      List("searchResultIndex")),
    UserDefinedColumn("udc_singer_or_radio_sid",
      udf(SingerRankRadioUtils.getRadioSingerFromPath: (String, String, String) => String),
      List("path", "contentType", "omnibusSid")),
    UserDefinedColumn("udc_mv_hot_key",
      udf(SingerRankRadioUtils.getRankFromPath: (String, String) => String),
      List("path", "contentType")),
    UserDefinedColumn("dim_date", udf(getDimDate: String => String), List("datetime")),
    UserDefinedColumn("dim_time", udf(getDimTime: String => String), List("datetime"))
  )

  columnsFromSource = List(
    ("duration", "duration"),
    ("program_duration", "cast(videoDuration as bigint)"),
    ("trailer_duration", "cast(trailerDuration as bigint)"),
    ("user_id", "userId"),
    ("product_sn", "productSN"),
    ("end_event", "event"),
    ("start_time", "cast(startTime as timestamp)"),
    ("end_time", "cast(endTime as timestamp)"),
    ("content_type", "case when udc_path_content_type is not null and trim(udc_path_content_type) = 'interest' then udc_path_content_type " +
        "else dim_whaley_program.content_type end"),
    ("play_content_type",
      "case when dim_whaley_subject.subject_content_type is not null then dim_whaley_subject.subject_content_type " +
        "when udc_path_content_type is not null and trim(udc_path_content_type) = 'interest' then udc_path_content_type " +
        "when dim_whaley_program.content_type is not null then dim_whaley_program.content_type " +
        "when trim(contentType) = '' then null else contentType end"),
    ("is_reservation", "case when trim(contentType) = 'reservation' then 'true' else 'false' end"),
    ("search_keyword", "searchText"),
    ("search_rec_keyword", "case when hotSearchWord is null or " +
      "trim(hotSearchWord) = '' then searchAssociationalWord else hotSearchWord end "),
    ("voice_search", "case when path like '%voicesearch%' then 'true' else 'false' end"),
    ("launcher_link_type", "linkType"),
    ("launcher_link_value", "linkValue"),
    ("position_type", "positionType"),
    ("trailer_process", "trailerProcess"),
    ("trailer_mark", "trailerMark"),
    ("current_resolution", "currentResolution"),
    ("program_resolution", "resolution"),
    ("current_vip_level", "currentVipLevel"),
    ("product_line", "case when productLine is null or trim(productLine) = '' then 'helios' else productLine end"),
    ("rom_version", "romVersion"),
    ("firmware_version", "firmwareVersion"),

    //        ("network_type", "networkType"),

//    ("path", "path"),
//    ("subject_code", "udc_subject_code"),
//    ("wui_version", "udc_wui_version"),
//    ("launcher_access_location", "udc_launcher_access_location"),
//    ("launcher_location_index", "udc_launcher_location_index"),
//    ("recommend_position", "udc_recommend_position"),
//    ("recommend_index", "udc_recommend_index"),
//    ("path_content_type", "udc_path_content_type"),
//    ("page_code", "udc_page_code"),
//    ("page_area_code", "udc_page_area_code"),
//    ("page_location_code", "udc_page_location_code"),
//    ("page_location_index", "udc_page_location_index"),
//    ("last_category", "udc_last_category"),
//    ("last_second_category", "udc_last_second_category"),
//    ("search_from", "udc_search_from"),
//    ("search_from_hot_word", "udc_search_from_hot_word"),
//    ("search_from_associational_word", "udc_search_from_associational_word"),
//    ("retrieval", "retrieval"),
//    ("search_tab", "searchTab"),
//    ("search_result_index", "udc_search_result_index"),
//    ("singer_or_radio_sid", "udc_singer_or_radio_sid"),
//    ("mv_hot_key", "udc_mv_hot_key"),
    ("dim_date", "dim_date"),
    ("dim_time", "dim_time")

  )

  dimensionsNeedInFact = List("dim_whaley_program", "dim_whaley_subject")

  dimensionColumns = List(
    //用户
    new DimensionColumn("dim_whaley_product_sn",
      List(DimensionJoinCondition(Map("productSN" -> "product_sn"))),
      List(("product_sn_sk", "product_sn_sk"), ("web_location_sk", "user_web_location_sk"))),

    //剧头
    new DimensionColumn("dim_whaley_program",
      List(DimensionJoinCondition(Map("videoSid" -> "sid"))), "program_sk"),

    //剧集
    new DimensionColumn("dim_whaley_program", "dim_whaley_program_episode",
      List(DimensionJoinCondition(Map("episodeSid" -> "sid"))), "program_sk", "episode_program_sk"),

    //账号
    new DimensionColumn("dim_whaley_account",
      List(DimensionJoinCondition(Map("accountId" -> "account_id"))), "account_sk"),

    //专题
    new DimensionColumn("dim_whaley_subject",
      List(DimensionJoinCondition(Map("udc_subject_code" -> "subject_code"))), "subject_sk"),

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


    //频道页入口
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
      List(
        DimensionJoinCondition(
          Map("udc_last_category" -> "last_first_code", "udc_last_second_category" -> "last_second_code")
        ),
        DimensionJoinCondition( //关联源数据中只包含一层站点树的情况
          Map("udc_last_second_category" -> "last_first_code"),
          null, null, "udc_last_category is null"
        ),
        DimensionJoinCondition( //关联源数据中体育第二层错误的
          Map("udc_last_second_category" -> "last_first_code"),
          "site_content_type = 'sports'"
        )
      ), "source_site_sk"),
    //筛选
    new DimensionColumn("dim_whaley_retrieval",
      List(DimensionJoinCondition(
        Map("retrieval" -> "retrieval_key", "udc_path_content_type" -> "content_type")
      )), "retrieval_sk"),
    //搜索
    new DimensionColumn("dim_whaley_search",
      List(DimensionJoinCondition(
        Map("searchTab" -> "search_tab", "udc_search_from" -> "search_from",
          "udc_search_from_hot_word" -> "search_from_hot_word",
          "udc_search_from_associational_word" -> "search_from_associational_word",
          "udc_search_result_index" -> "search_result_index")
      ),
        DimensionJoinCondition(
          Map("searchTab" -> "search_tab",
            "udc_search_from_hot_word" -> "search_from_hot_word",
            "udc_search_from_associational_word" -> "search_from_associational_word",
            "udc_search_result_index" -> "search_result_index"),
          "search_from = 'unknown'", null, "udc_search_from is not null"
        )), "search_sk"),

    //智能推荐
    new DimensionColumn("dim_whaley_recommend_position",
      List(
        //1.首页推荐
        DimensionJoinCondition(
          Map("udc_launcher_location_index" -> "recommend_slot_index"),
          "recommend_algorithm='未知' and recommend_position='portalrecommend'", null, s"udc_recommend_position is null"
        ),
        //2.其他推荐
        DimensionJoinCondition(
          Map("udc_recommend_position" -> "recommend_position",
            "udc_path_content_type" -> "recommend_position_type",
            "udc_recommend_index" -> "recommend_slot_index"
          ),
          "recommend_algorithm='未知'", null, null
        ),
        DimensionJoinCondition(
          Map("udc_recommend_position" -> "recommend_position",
            "contentType" -> "recommend_position_type",
            "udc_recommend_index" -> "recommend_slot_index"
          ),
          "recommend_algorithm='未知'", null, null
        )
      ),
      "recommend_position_sk"),

    //体育比赛
    new DimensionColumn("dim_whaley_sports_match",
      List(DimensionJoinCondition(Map("matchSid" -> "match_sid"))), "match_sk"),

    //音乐精选集
    new DimensionColumn("dim_whaley_mv_topic",
      List(DimensionJoinCondition(Map("omnibusSid" -> "mv_topic_sid"))), "mv_topic_sk"),

    //明星
    new DimensionColumn("dim_whaley_cast",
      List(DimensionJoinCondition(Map("starSid" -> "cast_sid"))), "cast_sk"),

    //歌手
    new DimensionColumn("dim_whaley_singer",
      List(DimensionJoinCondition(Map("udc_singer_or_radio_sid" -> "singer_sid"))), "singer_sk"),

    //电台
    new DimensionColumn("dim_whaley_radio",
      List(DimensionJoinCondition(Map("udc_singer_or_radio_sid" -> "radio_sid"))), "radio_sk"),

    //榜单
    new DimensionColumn("dim_whaley_mv_hot_list",
      List(DimensionJoinCondition(Map("udc_mv_hot_key" -> "mv_hot_rank_id"))), "mv_hot_sk"),

    //节目聚合维度
    new DimensionColumn("dim_whaley_link_type",
      List(
        DimensionJoinCondition(Map("matchSid" -> "link_value_code"), "link_type_code = 16"),
        DimensionJoinCondition(Map("omnibusSid" -> "link_value_code"), "link_type_code = 11"),
        DimensionJoinCondition(Map("udc_subject_code" -> "link_value_code"), "link_type_code = 4"),
        DimensionJoinCondition(Map("udc_singer_or_radio_sid" -> "link_value_code"), "link_type_code in (36, 37) "),
        DimensionJoinCondition(Map("udc_mv_hot_key" -> "link_value_code"), "link_type_code = 19")
      ), "link_type_sk"),

    //路径聚合维度
    new DimensionColumn("dim_whaley_area_source_agg",
      List(
        //语言搜索
        DimensionJoinCondition(Map(), "source_code = 'voice_search'", null, "path like '%voicesearch%'"),
        DimensionJoinCondition(
          Map("udc_last_category" -> "sub_module_code", "udc_last_second_category" -> "module_code"),
          "source_code = 'source_site'"),
        DimensionJoinCondition( //关联源数据中只包含一层站点树的情况
          Map("udc_last_second_category" -> "sub_module_code"),
          "source_code = 'source_site'", null, "udc_last_category is null"
        ),
        DimensionJoinCondition(Map("udc_page_code" -> "module_code", "udc_page_area_code" -> "sub_module_code"),
          "source_code = 'channel_entrance'"),
        DimensionJoinCondition(Map("udc_launcher_access_location" -> "sub_module_code"),
          "source_code = 'launcher_entrance'")
      ), "area_source_agg_sk")
  )

  override def filterRows(sourceDf: DataFrame): DataFrame = {
    sourceDf.where("playStatus != 'failure' and playStatus is not null " +
      "and (event != 'startplay' and event != 'pause' and event != 'resume')")
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
