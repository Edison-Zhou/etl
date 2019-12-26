package cn.whaley.datawarehouse.global

/**
  * Created by baozhiwang on 2017/3/7.
  */
trait LogConfig {
  val MORETV = "moretv"
  val MEDUSA = "medusa"
  val MERGER = "merger"
  val LOGINLOG = "loginlog"
  val DANMU = "danmu"
  val DBSNAPSHOT = "dbsnapshot"
  val WHALEY = "whaley"
  val VR = "vr"
  val EAGLE = "eagle"
  val MTVKIDSLOGIN = "mtvkidsloginlog"
  val ACTIVITY = "activity"
  val SUBJECT = "subject"
  val MEDUSA4X = "medusa4x"
  val UTVMORE = "utvmore"

  /**
    * 频道类型 */
  val CHANNEL_MOVIE="movie"
  val CHANNEL_KIDS = "kids"
  val CHANNEL_SPORTS = "sports"
  val CHANNEL_TV="tv"
  val CHANNEL_HOT="hot"
  val CHANNEL_GAME = "game"
  val CHANNEL_VARIETY_PROGRAM="zongyi"
  val CHANNEL_OPERA="xiqu"
  val CHANNEL_COMIC="comic"
  val CHANNEL_RECORD="jilu"
  val CHANNEL_MV="mv"
  val CHANNEL_VIP = "vipClub"
  val CHANNEL_VR = "vr"
  val CHANNEL_INTEREST = "interest"

  val CHANNEL_LIST = List(CHANNEL_MOVIE,CHANNEL_KIDS,CHANNEL_SPORTS,CHANNEL_TV,CHANNEL_HOT,CHANNEL_VARIETY_PROGRAM,
    CHANNEL_OPERA,CHANNEL_COMIC,CHANNEL_RECORD,CHANNEL_MV,CHANNEL_VIP,CHANNEL_VR,CHANNEL_INTEREST)
}
