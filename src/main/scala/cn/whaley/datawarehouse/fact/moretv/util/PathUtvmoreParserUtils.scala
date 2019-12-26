package cn.whaley.datawarehouse.fact.moretv.util

import cn.whaley.datawarehouse.fact.constant.UDFConstantDimension._
import org.apache.spark.sql.Row

import scala.util.control.Breaks._

/**
  * Created by zhu.bingxin on 20190508.
  * 优视猫的路径解析，部分适用于优视猫
  */
object PathUtvmoreParserUtils {

  private val recommendLogType = "other"
  private val recommendLogHomeType = "home"
  private val DETAIL_LIST = List(DETAILHOMEACTIVITY, SPORTDETAILACTIVITY)
  private val HOME_PAGE_LIST = List(RECHOMEACTIVITY, KIDSHOMEACTIVITY, VODACTIVITY, LAUNCHERACTIVITY)
  private val LIVE_PAGE_LIST = List(LIVECHANNELACTIVITY, SPORTLIVEACTIVITY, CAROUSELACTIVITY)
  private val SUBJECT_LIST = List(SUBJECTHOMEACTIVITY, GROUPSUBJECTACTIVITY)
  private val OPENSCREEN_LIST = List(OPENSCREENACTIVITY)


  /**
    * 如果最后一个页面为播放页面，则删除最后一个页面；否则，直接返回原始路径
    *
    * @param path 用户行为路径
    * @return 到达播放路径
    */
  def getPlayPath(path: Seq[Row]): Seq[Row] = {
    if (path.last.getAs[String](PAGE_TYPE) == PLAYACTIVITY) {
      path.dropRight(1) //如果最后的页面是播放页面，将播放页面从数组中删除
    } else path
  }

  /**
    * 获取路径中从后往前第N个页面的信息（N从1开始，PlayActivity不算）
    *
    * @param path
    * @param index
    * @param getValueType
    * @return
    */
  def getIndexPath(path: Seq[Row], index: Int, getValueType: String) = {
    // 要求path不为空
    require(path != null)
    val path2 = getPlayPath(path)
    //path中的数组长度必须大于index
    if (path2.length >= index) {
      val get_path = path2.reverse(index - 1)

      getValueType match {
        case LOCATION_INDEX => get_path.getAs[String](LOCATION_INDEX)
        case PAGE_TYPE => get_path.getAs[String](PAGE_TYPE)
        case PAGE_URL => get_path.getAs[String](PAGE_URL)
        case SUB_SUBJECT_CODE => get_path.getAs[String](SUB_SUBJECT_CODE)
        case LINK_VALUE => get_path.getAs[String](LINK_VALUE)
        case ACCESS_AREA => get_path.getAs[String](ACCESS_AREA)
        case SUB_ACCESS_AREA => get_path.getAs[String](SUB_ACCESS_AREA)
        case FILTER_CONDITION => get_path.getAs[String](FILTER_CONDITION)
        case PAGE_TAB => get_path.getAs[String](PAGE_TAB)
        case _ => null
      }
    }
    else null

  }


  /**
    * 获取当前播放页前一个页面的信息
    *
    * @param path
    * @param getValueType 需要获取的值名称
    * @return
    */
  def getLastPath(path: Seq[Row], getValueType: String) = {
    getIndexPath(path, 1, getValueType)
  }

  /**
    * 获取page_url中的元素信息，若page_url为空，则返回空字符串
    *
    * @param urlString
    * @param element
    * @return
    */
  def getUrlElement(urlString: String, element: String) = {
    var value: String = ""
    if (urlString != null) {
      breakable {
        urlString.split("&").foreach(url => {
          val kvs = url.split("=")
          if (kvs(0) == element && kvs.length > 1) { // 获取url中的element值
            value = kvs(1)
            break()
          }
        })
      }
    }
    value
  }

  /**
    * 获取推荐来源类型（主页或者其他）
    *
    * @param path
    * @param alg
    * @return
    */
  def getRecommendSourceType(path: Seq[Row], alg: String) = {
    // 要求path不为空
    require(path != null)
    val page_type = getLastPath(path, PAGE_TYPE)
    if (page_type == LAUNCHERACTIVITY && alg != null && alg != "") recommendLogHomeType
    else if (alg != null && alg != "") recommendLogType
    else null
  }


  /**
    * 获取上一节目的sid，针对详情页跳详情页产生的播放
    *
    * @param path
    * @return
    */
  def getPreviousSid(path: Seq[Row]) = {
    // 要求path不为空
    require(path != null)
    var sid: String = null
    if (DETAIL_LIST.contains(getIndexPath(path, 1, PAGE_TYPE)) && DETAIL_LIST.contains(getIndexPath(path, 2, PAGE_TYPE)))
      sid = getUrlElement(getIndexPath(path, 2, PAGE_URL), "sid") // 获取url中的sid值
    sid
  }


  /**
    * 获取筛选页筛选条件
    *
    * @param path
    * @param filter_condition
    */
  def getFilterInfo(path: Seq[Row], filter_condition: String) = {
    // 要求path不为空
    require(path != null)
    val page_type = getLastPath(path, PAGE_TYPE) //从播放页开始前一个页面的页面类型
    val page_type2 = getIndexPath(path, 2, PAGE_TYPE) //从播放页开始往前数第二个页面的页面类型
    val condition = getLastPath(path, FILTER_CONDITION)
    val condition2 = getIndexPath(path, 2, FILTER_CONDITION)
    var content_type: String = null
    if (page_type == FILTERACTIVITY && condition != null) {
      //      val page_url = getLastPath(path, PAGE_URL)

      breakable {
        getLastPath(path, PAGE_URL).split("&").foreach(url => {
          val kvs = url.split("=")
          if (kvs(0) == "contentType" && kvs.length > 1) { // 获取url中的contentType值
            content_type = kvs(1)
            break()
          }
        })
      }

      filter_condition match {
        case "sort" => condition.split("&")(4).split("=")(1) //排序
        case "area" => condition.split("&")(2).split("=")(1) //地区
        case "year" => condition.split("&")(3).split("=")(1) //年代
        case "tag" => condition.split("&")(1).split("=")(1) //类型
        case "content_type" => content_type //频道类型
      }
    }
    else if (page_type2 == FILTERACTIVITY && condition2 != null) {
      breakable {
        getLastPath(path, PAGE_URL).split("&").foreach(url => {
          val kvs = url.split("=")
          if (kvs(0) == "contentType" && kvs.length > 1) { // 获取url中的contentType值
            content_type = kvs(1)
            break()
          }
        })
      }

      filter_condition match {
        case "sort" => condition2.split("&")(4).split("=")(1) //排序
        case "area" => condition2.split("&")(2).split("=")(1) //地区
        case "year" => condition2.split("&")(3).split("=")(1) //年代
        case "tag" => condition2.split("&")(1).split("=")(1) //类型
        case "content_type" => content_type //频道类型
      }

    }
    else null

  }

  /**
    * 获取首页信息：依次是page_code,access_area,sub_access_area,location_index
    *
    * @param path
    * @return
    */
  def getLayoutPageInfo(path: Seq[Row]) = {
    // 要求path不为空
    require(path != null)

    var homePageCode: String = null
    var accessArea: String = null
    var subAccessArea: String = null
    var locationIndex: String = null
    var channelType: String = null

    val page_type = getLastPath(path, PAGE_TYPE)
    val access_area = getLastPath(path, ACCESS_AREA)
    val page_type2 = getIndexPath(path, 2, PAGE_TYPE)
    val access_area2 = getIndexPath(path, 2, ACCESS_AREA)
    val page_type3 = getIndexPath(path, 3, PAGE_TYPE)
    val access_area3 = getIndexPath(path, 3, ACCESS_AREA)
    val page_type4 = getIndexPath(path, 4, PAGE_TYPE)
    val access_area4 = getIndexPath(path, 4, ACCESS_AREA)
    val page_type5 = getIndexPath(path, 5, PAGE_TYPE)
    val access_area5 = getIndexPath(path, 5, ACCESS_AREA)

    //HOME_PAGE_LIST限制了页面为大首页、体育或者游戏首页、少儿首页以及通用频道页；access_area != null && access_area != ""限制了通用频道首页
    if (HOME_PAGE_LIST.contains(page_type) && access_area != null && access_area != "") {
      channelType = getUrlElement(getLastPath(path, PAGE_URL), "channelType") // 获取url中的channelType值
      homePageCode = page_type match {
        case LAUNCHERACTIVITY => "ysmHomePage1"
        case _ => channelType.concat("HomePage1")
      }
      accessArea = getLastPath(path, ACCESS_AREA)
      subAccessArea = getLastPath(path, SUB_ACCESS_AREA)
      locationIndex = getLastPath(path, LOCATION_INDEX)
    }
    //page->openScreenActivity
    else if (OPENSCREEN_LIST.contains(page_type2) && DETAIL_LIST.contains(page_type)) {
      homePageCode = "open_screen"
      accessArea = ""
      subAccessArea = ""
      locationIndex = ""
    }

    //page->detail/SmallVideoActivity(小视频专题页)
    else if ((DETAIL_LIST.contains(page_type) || page_type == SMALLVIDEOACTIVITY) //最后的页面是详情页或者小视频专题页
      && HOME_PAGE_LIST.contains(page_type2) && access_area2 != null && access_area2 != "") {
      channelType = getUrlElement(getIndexPath(path, 2, PAGE_URL), "channelType") // 获取url中的channelType值
      homePageCode = page_type2 match {
        case LAUNCHERACTIVITY => "ysmHomePage1"
        case _ => channelType.concat("HomePage1")
      }
      accessArea = getIndexPath(path, 2, ACCESS_AREA)
      subAccessArea = getIndexPath(path, 2, SUB_ACCESS_AREA)
      locationIndex = getIndexPath(path, 2, LOCATION_INDEX)
    }

    else if (
      ((DETAIL_LIST.contains(page_type) && DETAIL_LIST.contains(page_type2)) //page->detail->detail
        || (page_type == SUBJECTHOMEACTIVITY || page_type == GROUPSUBJECTACTIVITY)) //page->中间页->subject/groupSubject
        && HOME_PAGE_LIST.contains(page_type3) && access_area3 != null && access_area3 != "") {
      channelType = getUrlElement(getIndexPath(path, 3, PAGE_URL), "channelType") // 获取url中的channelType值
      homePageCode = page_type3 match {
        case LAUNCHERACTIVITY => "ysmHomePage1"
        case _ => channelType.concat("HomePage1")
      }
      accessArea = getIndexPath(path, 3, ACCESS_AREA)
      subAccessArea = getIndexPath(path, 3, SUB_ACCESS_AREA)
      locationIndex = getIndexPath(path, 3, LOCATION_INDEX)
    }

    //page->subject/groupSubject->detail
    else if (DETAIL_LIST.contains(page_type)
      && (page_type2 == SUBJECTHOMEACTIVITY || page_type2 == GROUPSUBJECTACTIVITY)
      && HOME_PAGE_LIST.contains(page_type4) && access_area4 != null && access_area4 != "") {
      channelType = getUrlElement(getIndexPath(path, 4, PAGE_URL), "channelType") // 获取url中的channelType值
      homePageCode = page_type4 match {
        case LAUNCHERACTIVITY => "ysmHomePage1"
        case _ => channelType.concat("HomePage1")
      }
      accessArea = getIndexPath(path, 4, ACCESS_AREA)
      subAccessArea = getIndexPath(path, 4, SUB_ACCESS_AREA)
      locationIndex = getIndexPath(path, 4, LOCATION_INDEX)
    }

    else if (DETAIL_LIST.contains(page_type) && DETAIL_LIST.contains(page_type2)
      && (page_type3 == SUBJECTHOMEACTIVITY || page_type3 == GROUPSUBJECTACTIVITY)
      && HOME_PAGE_LIST.contains(page_type5) && access_area5 != null && access_area5 != "") {
      channelType = getUrlElement(getIndexPath(path, 5, PAGE_URL), "channelType") // 获取url中的channelType值
      homePageCode = page_type5 match {
        case LAUNCHERACTIVITY => "ysmHomePage1"
        case _ => channelType.concat("HomePage1")
      }
      accessArea = getIndexPath(path, 5, ACCESS_AREA)
      subAccessArea = getIndexPath(path, 5, SUB_ACCESS_AREA)
      locationIndex = getIndexPath(path, 5, LOCATION_INDEX)
    }
    //page->直播页
    else if (LIVE_PAGE_LIST.contains(page_type) && HOME_PAGE_LIST.contains(page_type2)) {
      channelType = getUrlElement(getIndexPath(path, 2, PAGE_URL), "channelType") // 获取url中的channelType值
      //      if (channelType == null)
      //        throw new RuntimeException(UdfUtils.rowSeq2String(path))
      homePageCode = page_type2 match {
        case LAUNCHERACTIVITY => "ysmHomePage1"
        case _ => channelType.concat("HomePage1")
      }
      accessArea = getIndexPath(path, 2, ACCESS_AREA)
      subAccessArea = getIndexPath(path, 2, SUB_ACCESS_AREA)
      locationIndex = getIndexPath(path, 2, LOCATION_INDEX)
    }

    if (homePageCode == "memberHomePage1") homePageCode = "vipHomePage1" //针对会员首页pageCode做特殊处理
    List(homePageCode, accessArea, subAccessArea, locationIndex)
  }


  /**
    * 获取直播播放一级分类code，网络直播取的是tab_code码
    *
    * @param path
    * @return
    */
  def getLiveSourceSiteCode(path: Seq[Row]) = {
    // 要求path不为空
    require(path != null)
    var code: String = null
    for (elem <- 0 until path.length) {
      val page_type = path(elem).getAs[String](PAGE_TYPE)
      val page_tab = path(elem).getAs[String](PAGE_TAB)
      breakable {
        if (page_type == VODACTIVITY && page_tab != null) {
          code = page_tab
          break()
        }
      }
    }
    code
  }

  /**
    * 解析路径中的专题code，返回主专题code和子专题code
    *
    * @param path
    */
  def getLiveSubjectCode(path: Seq[Row]): List[String] = {
    println("start excute getLiveSubjectCode fuction")
    // 要求path不为空
    require(path != null)
    var main_subject_code: String = null //组合专题中的主专题code，非组合专题为null
    var subject_code: String = null //组合专题中的子专题code或者非组合专题code
    for (elem <- 0 until path.length) {
      val page_type = path(elem).getAs[String](PAGE_TYPE)
      val sub_subject_code = path(elem).getAs[String](SUB_SUBJECT_CODE)
      val page_url = path(elem).getAs[String](PAGE_URL)

      if (page_type == SUBJECTHOMEACTIVITY) subject_code = sub_subject_code //非组合专题，专题code就是子专题code
      else if (page_type == GROUPSUBJECTACTIVITY) {
        breakable {
          page_url.split("&").foreach(url => {
            val kvs = url.split("=")
            if (kvs(0) == "linkValue" && kvs.length > 1) { // 获取url中的linkValue值
              main_subject_code = kvs(1)
              break()
            }
          })
        }
        subject_code = sub_subject_code
      }
    }
    List(main_subject_code, subject_code)
  }

  /**
    * 获取各频道（content_type）播放入口页面
    * 需求：
    * 各频道总播放量的不同入口限定：
    * （1）4.0首页：包括4.0今日推荐（去除第二行位置3、位置4）、4.0首页短视频、4.0首页体育电竞、4.0首页独家策划、4.0首页会员看看。
    * （2）4.0观看历史：无论是从“我的”进入，还是从“今日推荐”进入，只要在观看历史产生的观看数据都计入。
    * （3）4.0收藏追看：无论是从“我的”进入，还是从“今日推荐”进入，只要在收藏追看产生的观看数据都计入。
    * （4）4.0分类入口：从4.0首页各分类入口进入产生的播放数据。
    * （5）4.0“我的”会员频道入口（只有会员频道有此入口）
    * （6）4.0其他
    *
    * @param path
    * @return
    */
  def getChannelPlayEntrance(path: Seq[Row]): String = {
    var result: String = null
    var entrance: Row = null
    var launAreaCode: String = null
    if (path.length >= 1 && path(0).schema.exists(_.name == "access_area")) {
      launAreaCode = path(0).getAs[String]("access_area")
    }

    require(path != null)
    val playPath = getPlayPath(path)

    /** 获取入口页面 **/
    //（长视频）专题--详情页--详情页--播放或 专题--详情页--详情页（直接播放）
    if (playPath.length >= 5 && getIndexPath(playPath, 1, PAGE_TYPE) == DETAILHOMEACTIVITY && getIndexPath(playPath, 2, PAGE_TYPE) == DETAILHOMEACTIVITY && getIndexPath(playPath, 3, PAGE_TYPE) == SUBJECTHOMEACTIVITY) {
      entrance = playPath(playPath.length - 5)
    }
    //（长视频）详情页--详情页--播放  或 详情页--详情页（直接播放）
    else if (playPath.length >= 3 && getIndexPath(playPath, 1, PAGE_TYPE) == DETAILHOMEACTIVITY && getIndexPath(playPath, 2, PAGE_TYPE) == DETAILHOMEACTIVITY) {
      entrance = playPath(playPath.length - 3)
    }
    //（长视频）专题--详情页--播放 或 专题--详情页（直接播放）
    else if (playPath.length >= 4 && getIndexPath(playPath, 1, PAGE_TYPE) == DETAILHOMEACTIVITY && getIndexPath(playPath, 2, PAGE_TYPE) == SUBJECTHOMEACTIVITY) {
      entrance = playPath(playPath.length - 4)
    }
    //（长视频） 详情页--播放 或 详情页（直接播放）
    else if (playPath.length >= 2 && getIndexPath(playPath, 1, PAGE_TYPE) == DETAILHOMEACTIVITY) {
      entrance = playPath(playPath.length - 2)
    }
    //短视频专题
    else if (playPath.length >= 3 && getIndexPath(playPath, 1, PAGE_TYPE) == SUBJECTHOMEACTIVITY) {
      entrance = playPath(playPath.length - 3)
    }
    //短视频组合专题
    else if (playPath.length >= 3 && getIndexPath(playPath, 1, PAGE_TYPE) == GROUPSUBJECTACTIVITY) {
      entrance = playPath(playPath.length - 3)
    }
    //add by huganglong 190322
    //小视频专题
    else if (playPath.length >= 2 && getIndexPath(playPath, 1, PAGE_TYPE) == SMALLVIDEOACTIVITY) {
      entrance = playPath(playPath.length - 2)
    }
    //短视频节目
    else if (playPath.length >= 1) {
      entrance = playPath(playPath.length - 1)
    }

    /** 解析上一级页面（来源页面） **/
    var entrancePage: String = ""
    if (entrance != null) {
      entrancePage = entrance.getAs[String]("page_type")
    }

    /** 获取treeSite **/
    var treeSite: String = ""
    breakable {
      if (entrance != null) {
        if (entrance.getAs[String]("page_url") != null) {
          entrance.getAs[String]("page_url").split("&").foreach(url => {
            val kvs = url.split("=")
            if (kvs(0) == "treeSite" && kvs.length > 1) {
              treeSite = kvs(1)
              break()
            }
          })
        }
      }
    }
    /** 获取channelType **/
    var channelType: String = ""
    breakable {
      if (entrance != null) {
        if (entrance.getAs[String]("page_url") != null) {
          entrance.getAs[String]("page_url").split("&").foreach(url => {
            val kvs = url.split("=")
            if (kvs(0) == "channelType" && kvs.length > 1) {
              channelType = kvs(1)
              break()
            }
          })
        }
      }
    }
    //channelType映射中文名称
    var channelName: String = channelType match {
      case "movie" => "电影分类入口"
      case "tv" => "电视剧分类入口"
      case "zongyi" => "综艺分类入口"
      case "kids" => "少儿分类入口"
      case "comic" => "动漫分类入口"
      case "game" => "游戏电竞分类入口"
      case "hot" => "资讯分类入口"
      case "interest" => "奇趣分类入口"
      case "sports" => "体育分类入口"
      case "member" => "会员分类入口"
      case "jilu" => "纪实分类入口"
      case "mv" => "音乐分类入口"
      case "cantonese" => "粤语分类入口"
      case "xiqu" => "戏曲分类入口"
      case _ => null
    }


    /** 依据各个页面得到对应入口信息 **/
    //大首页
    if (entrancePage == LAUNCHERACTIVITY && entrance.schema.exists(_.name == "link_style") && entrance.getAs[String]("link_style") != "launRecentlyLayout") {
      result = LAUNCHERACTIVITY
    }
    //4.0观看历史：无论是从“我的”进入，还是从“今日推荐”进入，只要在观看历史产生的观看数据都计入
    else if ((entrancePage == USERCENTERACTIVITY && entrance.getAs[String]("page_tab") == "观看历史") || (entrancePage == KIDSHOMEACTIVITY && entrance.schema.exists(_.name == "link_style") && entrance.getAs[String]("link_style") == "kidsRecentlyLayout") || (entrancePage == LAUNCHERACTIVITY && entrance.schema.exists(_.name == "link_style") && entrance.getAs[String]("link_style") == "launRecentlyLayout")) {
      result = "watch_history"
    }
    //4.0收藏追看：无论是从“我的”进入，还是从“今日推荐”进入，只要在收藏追看产生的观看数据都计入
    else if (entrancePage == USERCENTERACTIVITY && entrance.schema.exists(_.name == "page_tab") && entrance.getAs[String]("page_tab") == "收藏追看") {
      result = "collect_watch"
    }
    //add by huganglong 190322
    //4.0专题收藏
    else if (entrancePage == USERCENTERACTIVITY && entrance.schema.exists(_.name == "page_tab") && entrance.getAs[String]("page_tab") == "专题收藏") {
      result = "subject_collect"
    }
    //小视频专题奇趣分类入口数据
    else if (entrancePage == VODACTIVITY && treeSite != null && launAreaCode == "editor_funny") {
      result = channelName
    }
    //4.0分类入口：从4.0首页各分类入口进入产生的播放数据
    //（通用频道 :电影、电视剧、综艺、动漫、资讯、奇趣、会员、纪实、音乐、粤语
    //少儿的各专区页、少儿大首页、游戏体育大首页）
    else if ((entrancePage == VODACTIVITY || entrancePage == KIDSANIMACTIVITY || entrancePage == KIDSRHYMESACTIVITY || entrancePage == KIDSCOLLECTACTIVITY || entrancePage == KIDSHOMEACTIVITY || entrancePage == RECHOMEACTIVITY) && treeSite != null && launAreaCode == "editor_classification") {
      result = channelName
    }
    else if (entrancePage == VODACTIVITY && treeSite != null && launAreaCode == "editor_my" && channelType == "member") {
      result = "“我的”会员频道入口"
    }
    else {
      result = "其他"
    }
    result
  }

  /**
    * 根据path获取播放来源页
    *
    * @param path
    * @return
    */
  def getPlaySourcePage(path: Seq[Row]): String = {
    require(path != null)
    val playPath = getPlayPath(path)
    var entrance: Row = null

    // 1. 长视频专题 页面（历史、搜索）<-- 专题中转页面 <-- 专题页 <-- 详情页 ||  页面（历史、搜索）<-- 专题中转页面 <-- 专题页 <-- 详情页 <-- 播放页
    if (playPath.length >= 4 && getIndexPath(playPath, 1, PAGE_TYPE) == DETAILHOMEACTIVITY
      && getIndexPath(playPath, 2, PAGE_TYPE) == SUBJECTHOMEACTIVITY
      && getIndexPath(playPath, 3, PAGE_TYPE) == SUBJECTTRANSFERACTIVITY) {
      entrance = playPath(playPath.length - 4)
    }
    // 2. 短视频专题 页面（历史、搜索）<-- 专题中转页面 <-- 专题页  ||  页面（历史、搜索）<-- 专题中转页面 <-- 专题页
    else if (playPath.length >= 3 && getIndexPath(playPath, 1, PAGE_TYPE) == SUBJECTHOMEACTIVITY
      && getIndexPath(playPath, 2, PAGE_TYPE) == SUBJECTTRANSFERACTIVITY) {
      entrance = playPath(playPath.length - 3)
    }
    // 3. 组合专题 页面（历史、搜索）<-- 专题中转页面 <-- 组合专题页  ||  页面（历史、搜索）<-- 专题中转页面 <-- 组合专题页
    else if (playPath.length >= 3 && getIndexPath(playPath, 1, PAGE_TYPE) == GROUPSUBJECTACTIVITY
      && getIndexPath(playPath, 2, PAGE_TYPE) == SUBJECTTRANSFERACTIVITY) {
      entrance = playPath(playPath.length - 3)
    }
    // 4. 长视频  页面（历史、搜索）<-- 详情页  ||  页面（历史、搜索）<-- 详情页 <-- 播放页
    else if (playPath.length >= 2 && getIndexPath(playPath, 1, PAGE_TYPE) == DETAILHOMEACTIVITY) {
      entrance = playPath(playPath.length - 2)
    }
    // 5. 短视频节目 页面（历史、搜索）<-- 播放页  ||  页面（历史、搜索）<-- 播放页
    else if (playPath.length >= 1) {
      entrance = playPath(playPath.length - 1)
    }
    /** 解析上一级页面（来源页面） **/
    var entrancePage: String = null
    if (entrance != null) {
      entrancePage = entrance.getAs[String]("page_type")
    }
    entrancePage
  }

  // add by wangning-20190419
  def getCarouselEntrance(path: Seq[Row]): String = {
    var result: String = null
    var entrance: Row = null

    if (path != null) {
      // page:LauncherActivity-CarouselActivity
      if (path.length >= 2 && path(path.length - 1).getAs[String]("page_type") == "CarouselActivity" && path(path.length - 2).getAs[String]("page_type") == "LauncherActivity") {
        entrance = path(path.length - 2)
      }
      //page:LauncherActivity-RecHomeActivity-CarouselActivity
      else if (path.length >= 3 && path(path.length - 1).getAs("page_type") == "CarouselActivity" && path(path.length - 2).getAs("page_type") == "RecHomeActivity" && path(path.length - 3).getAs("page_type") == "LauncherActivity") {
        entrance = path(path.length - 2)
      }
    }


    //解析轮播播放页上一个页面
    var entrancePage: String = ""
    var entranceLocationIndex: String = ""
    var entranceSubArea: String = ""
    if (entrance != null) {
      entrancePage = entrance.getAs[String]("page_type")
      entranceLocationIndex = entrance.getAs[String]("location_index")
      entranceSubArea = entrance.getAs[String]("sub_access_area")
    }

    //获取channelType
    var channelType: String = ""
    breakable {
      if (entrance != null) {
        if (entrance.getAs[String]("page_url") != null) {
          entrance.getAs[String]("page_url").split("&").foreach(url => {
            val kvs = url.split("=")
            if (kvs(0) == "channelType" && kvs.length > 1) {
              channelType = kvs(1)
              break()
            }
          })
        }
      }
    }

    if (entrancePage == "LauncherActivity" && entranceSubArea == "editor_Live2" && entranceLocationIndex == "1") {
      result = entrancePage + "Carousel"
    }
    else if (entrancePage == "LauncherActivity" && (entranceSubArea == "editor_Live1" || (entranceSubArea == "editor_Live2" && (entranceLocationIndex == "3" || entranceLocationIndex == "4")))) {

      result = entrancePage + "LiveRecommend"
    }
    else if (entrancePage == "LauncherActivity") {
      result = entrancePage
    }
    else if (entrancePage == "RecHomeActivity") {
      result = entrancePage + channelType
    }
    result
  }

  def getEntrancePageCode(path_4x: Seq[Row]): String = {
    var pageCode: String = null
    pageCode = "ysmHomePage1"
    pageCode
  }

}

