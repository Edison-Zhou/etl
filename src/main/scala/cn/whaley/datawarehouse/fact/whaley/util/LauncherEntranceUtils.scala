package cn.whaley.datawarehouse.fact.whaley.util

/**
  * Created by zhangyu on 17/5/16.
  * 解析微鲸播放日志首页入口维度的几个指标
  */
object LauncherEntranceUtils {

  /**
    * 获取WUI首页各行的入口维度
    *
    * @param path
    * @param linkValue
    * @return
    */

  def launcherAccessLocationFromPath(path: String, linkValue: String): String = {
    if (path == null || path.isEmpty) {
      null
    } else {
      val tmp = path.split("-")
      if (tmp.length >= 2) {
        val secondPath = tmp(1)
        getAccessLocation(secondPath, linkValue)
      } else null
    }
  }

  def launcherAccessAreaFromPlayPath(path: String): String = {
    if (path == null || path.isEmpty) {
      null
    } else {
      val tmp = path.split("-")
      if (tmp.length >= 2) {
        val secondPath = tmp(1)
        getAccessAreaCodeFromPlay(secondPath)
      } else null
    }
  }

  def launcherLocationIndexFromPlay(path: String, recommendLocation: String): Int = {
    if (path == null || path.isEmpty) {
      -1
    } else {
      val tmp = path.split("-")
      if (tmp.length >= 2) {
        val secondPath = tmp(1)
        getLocationIndexFromPlay(secondPath, recommendLocation)
      } else -1
    }
  }

  /**
    * 将 发现 一行的首页入口替换为具体的榜单信息
    * 处理hot11的路径bug(home-hot11)
    *
    * @param secondPath
    * @param linkValue
    * @return
    */

  def getAccessLocation(secondPath: String, linkValue: String): String = {
    if (secondPath == "top") {
      linkValue
    } else if (secondPath == "hot11") {
      "recommendation"
    } else secondPath
  }

  /**
   *  仅限从播放日志中分析首页的accessArea,不能适用于点击等其他行为
   */
  def getAccessAreaCodeFromPlay(secondPath: String):String = {
    //处理历史/收藏/账户中心/自定义电视等
    if(secondPath == "watching_history" || secondPath == "collection" || secondPath == "account" ||
      secondPath == "history" || secondPath == "collect" || secondPath == "my_tv"){
      "my_tv"
    }else if (secondPath.startsWith("top")){
      "discover"
    }else if(secondPath.startsWith("today_recommend")){
      "today_recommend"
    }else if(secondPath == "hot11" || secondPath == "recommendation"){
      "recommendation"
    }else if(secondPath == "vip_recommend" || secondPath == "movie_recommend" ||
        secondPath == "children_recommend" || secondPath == "variety_recommend"){
      secondPath
    } else "classification"
  }

  /**
    * 获取WUI首页今日推荐/精选推荐的推荐位
    *
    * @param recommendLocation
    * @return
    */

  def getLocationIndexFromPlay(secondPath:String,recommendLocation: String): Int = {
    if (recommendLocation == null || recommendLocation.isEmpty
      || !secondPath.contains("recommend")) {
      -1
    } else {
      if (recommendLocation.contains("-")) {
        //剔除01版本中的大小推荐位信息
        val tmp = recommendLocation.split("-")
        tmp(0).toInt + 1
      } else {
        recommendLocation.toInt + 1
      }
    }
  }

  /**
    * 根据首页点击日志获取首页的Area
    * newModule5 表示分类，newModule1表示推荐，newModule2表示精选
    * @param romVersion
    * @param firmwareVersion
    * @param page
    * @param areaName
    * @param locationCode
    * @return
    */
  def getLauncherAreaFromClick(romVersion: String, firmwareVersion: String, page: String, areaName: String, locationCode: String,
                                   locationIndex: String,flag:String): String = {
    try {
      if (page == "home" && flag == "one") {
        if (LauncherEntranceUtils.wuiVersionFromPlay(romVersion, firmwareVersion) == "02") {
          if (areaName == "我的电视")  "my_tv"
          else if (areaName == "newModule1" ) "today_recommend"
          else if (areaName == "newModule2" && locationCode == "top")  "discover"
          else if (areaName == "newModule5"||areaName == "newModule3") "classification"
          else if (areaName == "signal") "signal_source"
          else if (areaName == "search") "search"
          else     locationCode
        } else {
          areaName
        }
      } else null
    } catch {
      case ex: Exception => ""
    }
  }



  /**
    * 根据首页点击日志获取首页的Location
    *
    * @param romVersion
    * @param firmwareVersion
    * @param page
    * @param areaName
    * @param locationCode
    * @param linkValue
    * @return
    */
  def getLauncherLocationFromClick(romVersion: String, firmwareVersion: String, page: String, areaName: String, locationCode: String,
                                   linkValue: String, locationIndex: String,flag:String): String = {
    try {
      if (page == "home" && flag == "one") {
        if (LauncherEntranceUtils.wuiVersionFromPlay(romVersion, firmwareVersion) == "01") {
          if (areaName == "signal_source") "signal"
          else if (areaName == "my_tv") {
            if (locationCode == "watching_history") "watching_history"
            else if (locationCode == "collection") "collection"
            else if (locationCode == "account") "account"
            else "my_tv"
          }
          else if (areaName == "classification") {
            if (locationCode == "quanjing") "vr"
            else if (locationCode == "clubmember") "vipClub"
            else locationCode
          }
          else if (areaName == "application") {
            if (locationCode == "media_player") "media_play"
            else locationCode

          }
          else if (areaName == "recommendation") "recommendation"
          else if (locationCode == null || locationCode.isEmpty) null
          else locationCode

        } else {
          if ((areaName == "newModule5" || areaName == "newModule3") && linkValue=="site_clubmember") "vipClub"
          else if ((areaName == "newModule5" || areaName == "newModule3") && linkValue=="site_vip" )  "vipClub"
          else if ((areaName == "newModule5" || areaName == "newModule3") && linkValue=="site_sport" )  "sports"
          else if  (areaName == "newModule5" )  linkValue.split("_")(1)
          else if (areaName == "signal" || areaName == "search") areaName
          else if (areaName == "我的电视") {
            if (locationIndex.toInt == 1) "history"
            else if (locationIndex.toInt == 2) "collect"
            else if (locationIndex.toInt == 3) "account"
            else "my_tv"
          }
          else if (locationCode == "top") linkValue
          else if (locationCode == null || locationCode.isEmpty) null
          else locationCode
        }

      } else null
    } catch {
      case ex: Exception => ""
    }
  }

  /**
    * 根据首页点击日志获取首页的索引
    *
    * @param page
    * @param romVersion
    * @param firmwareVersion
    * @param areaName
    * @param locationIndex
    * @return
    */
  def launcherLocationIndexFromClick(page: String, romVersion: String, firmwareVersion: String, areaName: String,
                                     locationIndex: String,flag:String): Int = {

    if (page == "home" && flag == "one") {
      if (locationIndex == null || locationIndex.isEmpty) -1
      else {
        if (LauncherEntranceUtils.wuiVersionFromPlay(romVersion, firmwareVersion) == "01") {
          if (areaName != "recommendation") -1
          else locationIndex.toInt + 1
        } else {
          if (areaName == "分类" || areaName == "我的电视" || areaName == "signal" || areaName == "search") -1
          else locationIndex.toInt + 1
        }
      }
    } else -2

  }

  /**
    * 获取首页WUI版本
    *
    * @param romVersion
    * @return
    */
  def wuiVersionFromPlay(romVersion: String, firmwareVersion: String): String = {
    val wui = RomVersionUtils.getRomVersion(romVersion, firmwareVersion)
    if (wui == null || wui.isEmpty) {
      null
    } else {
      val startIndex = wui.indexOf(".")
      if (startIndex > 0) {
        val wuiVersion = wui.substring(0, startIndex)
        val tmp = wui.split('.')
        if (tmp.length == 4 || wuiVersion == "02") {
          "02"
        } else if (wuiVersion == "00" || wuiVersion == "01") {
          "01" //将00版本替换为01版本
        } else null
      } else null
    }
  }

  /**
   * 获取vod20中首页accessarea
   * @param elementcode
   * @return
   */
  def launcherAccessAreaFromPlayVod(elementcode:String,flag:String):String = {
    if(elementcode == null || elementcode.isEmpty || flag != "two"){
      null
    }else{
      if(elementcode.startsWith("today_recommend")){
        "today_recommend"
      }else if(elementcode.startsWith("Site")){
        "classification"
      }else if(elementcode == "top"){
        "discover"
      }else elementcode
    }
  }

  /**
   * 获取vod20首页的accesslocation,我的电视/排行榜/分类部分用linkvalue值
   * @param elementcode
   * @param linkvalue
   * @return
   */
  def launcherAccessLocationFromPlayVod(elementcode:String,linkvalue:String,flag:String):String = {
    if(elementcode == null || elementcode.isEmpty || flag != "two"){
      null
    }else{
      if(elementcode == "my_tv" || elementcode == "top") linkvalue else if(elementcode.startsWith("Site")){
        if(linkvalue != null && linkvalue.split("_").length >= 2)
          linkvalue.split("_")(1)
        else
          null
      }else elementcode
    }
  }


  /**
   * 获取vod20首页点击的positionindex,只有精选(含发现)/今日推荐保留索引值,其余均为-1
   * @param elementcode
   * @param positionindex
   * @return
   */
  def launcherLocationIndexFromPlayVod(tablecode:String,elementcode:String,positionindex:String,flag:String):Int = {
    if(elementcode == null || elementcode.isEmpty || flag != "two") -2 else{
      if((tablecode == "newModule2" || elementcode.startsWith("today_recommend"))&& positionindex != null){
        positionindex.toInt + 1
      }else -1
    }
  }


}
