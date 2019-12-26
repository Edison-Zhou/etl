package cn.whaley.datawarehouse.global

/**
 * Created by zhangyu on 18/4/12.
 */
trait LinkType {

  //1 视频 11 音乐精选集 19 榜单 30 比赛 36 电台 37 热门歌手 45 FM电台 44 音乐个性化榜单
  val PROGRAM = "1"
  val MVTOPIC = "11"
  val MVHOTLIST = "19"
  val SPORTSMATCH = "30"
  val RADIO = "36"
  val SINGER = "37"
}
