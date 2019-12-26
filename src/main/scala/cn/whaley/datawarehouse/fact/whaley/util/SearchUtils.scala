package cn.whaley.datawarehouse.fact.whaley.util

/**
  * Created by Tony on 17/5/18.
  */
object SearchUtils {

  def getSearchFrom(path: String):String = {
    if(path == null || path.indexOf("-search") == -1) {
      null
    } else if(path.indexOf("home-search") == 0){
      "home"
    }else {
      val tmp = path.split("-")
      if (tmp.length >= 2) {
        tmp(1)
      } else "未知"
    }
  }

  def getSearchResultIndex(searchResultIndex: String):Int = {
    try {
      if (searchResultIndex == null || searchResultIndex.isEmpty) {
        -1
      } else if (searchResultIndex.trim.toInt + 1 > 100) {
        -2
      } else {
        searchResultIndex.trim.toInt + 1
      }
    }catch {
      case ex: Exception => -1
    }
  }

  def isHotSearchWord(hotSearchWord: String): Int = {
    if(hotSearchWord == null || hotSearchWord.isEmpty) {
      0
    } else {
      1
    }
  }

  def isAssociationalSearchWord(searchAssociationalWord: String): Int = {
    if(searchAssociationalWord == null || searchAssociationalWord.isEmpty) {
      0
    } else {
      1
    }
  }

  /**
   * 判断vod20中是否由语音搜索导致播放
   * @param parseStr
   * @return
   */
  def isVoiceSearchFromPlayVod(parseStr:String):String = {
    if(parseStr != null) "true" else "false"
  }


  def isHotSearchWordFromPlayVod(allsearchValue:String):Int = {
    if(allsearchValue != null && allsearchValue.isEmpty) 1 else 0
  }

  def getSearchResultIndexFromPlayVod(allsearchIndex:String,searchIndex:String):Int = {
    try{
      if(allsearchIndex == null || allsearchIndex.isEmpty || allsearchIndex.toInt == -1){
        if(searchIndex == null || searchIndex.isEmpty || searchIndex.toInt == -1) -1
        else if(searchIndex.toInt +1 > 100) -2
        else searchIndex.toInt + 1
      }else if(allsearchIndex.toInt + 1 > 10) -2
      else allsearchIndex.toInt +1
    }catch{
      case e:Exception => -1
    }
  }

  def isAssociationalSearchWordFromPlayVod(associatateWord:String):Int = {
    if(associatateWord == null || associatateWord.isEmpty) 0 else 1
  }

  def getSearchFromVod(searchLinkValue:String,sitetreeContentType:String):String = {
    if(searchLinkValue == null || searchLinkValue.isEmpty) null
    else{
      if(sitetreeContentType == null || sitetreeContentType.isEmpty) "home"
      else if (sitetreeContentType == "none") "unknown"
      else sitetreeContentType
    }
  }



}
