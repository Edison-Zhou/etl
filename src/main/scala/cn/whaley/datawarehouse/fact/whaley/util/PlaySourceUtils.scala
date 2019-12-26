package cn.whaley.datawarehouse.fact.whaley.util

/**
 * Created by zhangyu on 18/4/12.
 * vod20中判断播放事实的来源
 */
object PlaySourceUtils {

  //专题/标签/明星/退出推荐/详情页推荐/普通播放
  def getPlaySourceFromPlayVod(subjectLinkValue:String,tagLinkValue:String,starLinkaValue:String,
                                playexitLinkvalue:String,detailLinkValue:String):String = {

    if(subjectLinkValue != null) "subject"
    else if(tagLinkValue != null) "tag"
    else if(starLinkaValue != null) "star"
    else if(playexitLinkvalue != null) "playexit_recommend"
    else if(detailLinkValue != null) "detail_recommend"
    else "normal"
  }
}
