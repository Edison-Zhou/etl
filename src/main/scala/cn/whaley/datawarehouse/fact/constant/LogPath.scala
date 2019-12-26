package cn.whaley.datawarehouse.fact.constant

/**
  * Created by Tony on 17/4/5.
  */
object LogPath {

  val DATE_ESCAPE = "#{startDate}"
  val MEDUSA_LOGIN_LOG_PATH = s"/log/moretvloginlog/parquet/$DATE_ESCAPE/loginlog"
  val HELIOS_WHALEYVIP_GETBUYVIPPROCESS = s"/log/whaley/parquet/$DATE_ESCAPE/helios-whaleyvip-getBuyVipProcess"
  val HELIOS_ON = s"/log/whaley/parquet/$DATE_ESCAPE/on"
  val HELIOS_OFF = s"/log/whaley/parquet/$DATE_ESCAPE/off"
  val HELIOS_PLAY = s"/log/whaley/parquet/$DATE_ESCAPE/play"
  val HELIOS_DETAIL = s"/log/whaley/parquet/$DATE_ESCAPE/detail"
  val HELIOS_LAUNCHER = s"/log/whaley/parquet/$DATE_ESCAPE/launcher"
  val HELIOS_WUI_LAUNCHER = s"/log/boikgpokn78sb95kjhfrendoj8ilnoi7/parquet/$DATE_ESCAPE/positionClick"
  val HELIOS_WUI_BUTTON = s"/log/boikgpokn78sb95kjhfrendoj8ilnoi7/parquet/$DATE_ESCAPE/buttonClick"
  val HELIOS_CHANNEL_Click = s"/log/whaley/parquet/$DATE_ESCAPE/helios-channelhome-click"
  val HELIOS_MOVIE_Click = s"/log/whaley/parquet/$DATE_ESCAPE/helios-whaleymovie-moviehomeaccess"

  //微鲸播放质量日志
  val HELIOS_GET_VIDEO = s"/log/whaley/parquet/$DATE_ESCAPE/helios-player-sdk-getVideoInfo"
  val HELIOS_START_PLAY = s"/log/whaley/parquet/$DATE_ESCAPE/helios-player-sdk-startPlay"
  val HELIOS_PARSE = s"/log/whaley/parquet/$DATE_ESCAPE/helios-player-sdk-inner_outer_auth_parse"
  val HELIOS_BUFFER = s"/log/whaley/parquet/$DATE_ESCAPE/helios-player-sdk-buffer"
  val HELIOS_END_PLAY = s"/log/whaley/parquet/$DATE_ESCAPE/helios-player-sdk-endPlay"

  //微鲸站点树维度表
  val DIM_WHALEY_SOURCE_SITE = s"/data_warehouse/dw_dimensions/dim_whaley_source_site"
  //首页入口维度表
  val DIM_WHALEY_LAUNCHER_ENTRANCE = s"/data_warehouse/dw_dimensions/dim_whaley_launcher_entrance"
  //各频道入口维度表
  val DIM_WHALEY_PAGE_ENTRANCE = s"/data_warehouse/dw_dimensions/dim_whaley_page_entrance"

  val DIM_WHALEY_SUBJECT = s"/data_warehouse/dw_dimensions/dim_whaley_subject"
  val DIM_WHALEY_MV_TOPIC = s"/data_warehouse/dw_dimensions/dim_whaley_mv_topic"
  val DIM_WHALEY_MV_HOT_LIST = s"/data_warehouse/dw_dimensions/dim_whaley_mv_hot_list"
  val DIM_WHALEY_SINGER = s"/data_warehouse/dw_dimensions/dim_whaley_singer"
  val DIM_WHALEY_RADIO = s"/data_warehouse/dw_dimensions/dim_whaley_radio"
  val DIM_WHALEY_SPOTRS_MATCH = s"/data_warehouse/dw_dimensions/dim_whaley_sports_match"

  val MEDUSA_PLAY_BUFFER = s"/log/medusa/parquet/$DATE_ESCAPE/medusa-player-sdk-buffer"
  val MEDUSA_PLAY_START = s"/log/medusa/parquet/$DATE_ESCAPE/medusa-player-sdk-startPlay"
  val MEDUSA_PLAY_END = s"/log/medusa/parquet/$DATE_ESCAPE/medusa-player-sdk-endPlay"
  val MEDUSA_PLAY_VIDEO_INFO = s"/log/medusa/parquet/$DATE_ESCAPE/medusa-player-sdk-getVideoInfo"
  val MEDUSA_PLAY_PARSE = s"/log/medusa/parquet/$DATE_ESCAPE/medusa-player-sdk-inner_outer_auth_parse"

  val MEDUSA_PLAY = s"/log/medusa/parquet/$DATE_ESCAPE/play"
  val MORETV_PLAYVIEW = s"/mbi/parquet/playview/$DATE_ESCAPE"
  val MEDUSA_DETAIL = s"/log/medusa/parquet/$DATE_ESCAPE/detail"

  //电视猫曝光来源
  val MEDUSA_LAUNCHER_EXPOSE = s"/log/medusa//parquet/$DATE_ESCAPE/medusa-launcher-expose"
  val MEDUSA_CHANNEL_EXPOSE = s"/log/medusa//parquet/$DATE_ESCAPE/medusa-channel-expose"
  val MEDUSA_PLAY_BACK = s"/log/medusa//parquet/$DATE_ESCAPE/medusa-play-back"

  //电视猫点击
  val MEDUSA_LAUNCHER_CLICK = s"/log/medusa/parquet/$DATE_ESCAPE/homeaccess"

  //电视猫账号登录活跃日志
  val MEDUSA_ACCOUNT_LOGIN = s"/log/medusa/parquet/${DATE_ESCAPE}/mtvaccount"
  val MEDUSA_ACCOUNT_UID_MAP = "/data_warehouse/dw_normalized/medusa_account_uid_mapping"

  // 电视猫会员商品维度表
  val DIM_MEDUSA_MEMBER_GOOD = "/data_warehouse/dw_dimensions/dim_medusa_member_goods"

  // 电视猫账号维度表
  val MEDUSA_ACCOUNT = "/data_warehouse/dw_dimensions/dim_medusa_account"


  // 电视猫订单日志
  val MEDUSA_BUSINESS_ORDER = s"/data_warehouse/ods_view.db/db_snapshot_medusa_business_order/key_day=${DATE_ESCAPE}"

  // 电视猫订单事实表
  val FACT_MEDUSA_ORDER = s"/data_warehouse/dw_facts/fact_medusa_member_order/${DATE_ESCAPE}/*"

  // 电视猫订单&购买入口映射表
  val ORDER_ENTRANCE_UID_MAPPED = "/data_warehouse/dw_normalized/medusa_order_uid_entrance_mapping"


  // 电视猫会员购买页面入口吊起日志
  val MEDUSA_PURCHASE_ENTRANCE = s"/log/medusa/parquet/${DATE_ESCAPE}/medusa-vipentrance-click/*"

  // 腾讯sid转换为微鲸sid
  val TENCENT_CID_2_SID = s"/user/hive/warehouse/ai.db/dim_qqid2sid/product_line=moretv/key_time=${DATE_ESCAPE}"
  val TENCENT_CID_2_SID_ALL = "/user/hive/warehouse/ai.db/dim_qqid2sid/product_line=moretv/*/"

}
