package cn.whaley.datawarehouse.fact.whaley.util

import cn.whaley.datawarehouse.global.LinkType

/**
 * Created by zhangyu on 18/4/12.
 */
object VideoTypeUtils extends LinkType{

  def getVideoSidFromPlayVod(videoSid:String,linkType:String):String = {
    if(linkType == PROGRAM) videoSid else null
  }

  def getEpisodeSidFromPlayVod(episodeSid:String,linkType:String):String = {
    if(linkType == PROGRAM) episodeSid else null
  }

  def getOmnibusSidFromPlayVod(videoSid:String,linkType:String):String = {
    if(linkType == MVTOPIC) videoSid else null
  }

  def getRankSidFromPlayVod(videoSid:String,linkType:String):String = {
    if(linkType == MVHOTLIST) videoSid else null
  }

  def getMatchSidFromPlayVod(videoSid:String,linkType:String):String = {
    if(linkType == SPORTSMATCH) videoSid else null
  }

  def getRadioSidFromPlayVod(videoSid:String,linkType:String):String = {
    if(linkType == RADIO) videoSid else null
  }

  def getSingerSidFromPlayVod(videoSid:String,linkType:String):String = {
    if(linkType == SINGER) videoSid else null
  }


}
