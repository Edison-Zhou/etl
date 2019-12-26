package cn.whaley.datawarehouse.fact.moretv.util

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}
import cn.whaley.datawarehouse.global.LogConfig
import org.apache.avro.TestAnnotation

/**
  * Created by wu.jiulin on 2017/5/2.
  *
  * 频道主页来源维度(少儿，音乐，体育，奇趣，资讯，电竞)
  * 解析出pathMain或者path里的area_code和location_code，然后关联dim_medusa_page_entrance维度表，获得代理键
  * 1.kids和sports没有location_code. mv下的mvTopHomePage和mvRecommendHomePage有一部分location_code is null的情况,需要特殊处理
  * 2.kids的路径解析不出dim_medusa_page_entrance维度表的area_code,需要根据medusa_path_program_site_code_map维度表获得
  * 3.kids有medusa和moretv,mv和sports只有medusa
  * 4.mvRecommendHomePage下的mv_station需要特殊处理
  * 5.mvTopHomePage下的rege,xinge,biaoshen需要特殊处理
  */
object PageEntrancePathParseUtils extends LogConfig {

  private val PAGE_ENTRANCE_KIDS_REGEX = (".*(kids_home)-([A-Za-z_]*)").r
  private val PAGE_ENTRANGE_INTEREST_REGEX =(".*(interest-interest|interest-home)\\*(.*[\\u4e00-\\u9fa5])").r
  private val PAGE_ENTRANGE_GAME_REGEX = (".*(game-game)\\*(.*[A-Za-z_][A-Za-z])\\*(.*[1-9])").r
  private val PAGE_ENTRANGE_HOT_REGEX =(".*(hot-hot|hot-home)\\*(.*[\\u4e00-\\u9fa5])").r
  private val PAGE_ENTRANCE_MV_REGEX = (".*(mv)\\*([A-Za-z_]*)\\*([a-zA-Z_]*)").r
  private val PAGE_ENTRANCE_SPORTS_REGEX = (".*(sports)\\*([A-Za-z_]*)").r
  private val PAGE_ENTRANCE_LOCATION_CODE_LIST = List("personal_recommend", "site_mvsubject", "biaoshen", "site_concert", "site_dance", "site_mvyear", "site_collect", "site_mvarea", "site_mvstyle", "mv_station", "xinge", "rege", "site_hotsinger", "search")
  //  private val PAGE_ENTRANCE_REGEX = (".*(mv|kids_home|sports)[\\*-]{1,}([A-Za-z_]*)[\\*-]{1,}([a-zA-Z_]*)").r
  //  private val PAGE_ENTRANCE_WITHOUT_LOCATION_CODE_REGEX = (".*(mv|kids_home|sports)[\\*-]{1,}([A-Za-z_]*)").r

  def getPageEntrancePageCode(pathMain: String, path: String, pathSub: String, contentType:  String,flag: String): String = {
    var pageCode: String = null
    val code = "page"
    flag match {
      case MEDUSA =>
        pageCode = getPageEntranceCodeByPathETL(pathMain, pathSub,contentType,flag, code)
      case MORETV =>
        pageCode = getPageEntranceCodeByPathETL(path, "","",flag, code)
    }
    pageCode
  }

  def getPageEntranceAreaCode(pathMain: String, path: String, pathSub: String, contentType:  String,flag: String): String = {
    var areaCode: String = null
    val code = "area"
    flag match {
      case MEDUSA =>
        areaCode = getPageEntranceCodeByPathETL(pathMain, pathSub,contentType,flag, code)
      case MORETV =>
        areaCode = getPageEntranceCodeByPathETL(path, "","",flag, code)
    }
    areaCode
  }

  def getPageEntranceLocationCode(pathMain: String, path: String, pathSub: String, contentType:  String,flag: String): String = {
    var locationCode: String = null
    val code = "location"
    flag match {
      case MEDUSA =>
        locationCode = getPageEntranceCodeByPathETL(pathMain, pathSub,contentType,flag, code)
      case MORETV =>
        locationCode = getPageEntranceCodeByPathETL(path, "","",flag, code)
    }
    locationCode
  }

  def getPageEntranceCodeByPathETL(path: String, pathSub: String, contentType: String,flag: String, code: String): String = {
    var result: String = null
    var page_code: String = null
    var area_code: String = null
    var location_code: String = null
    if (path == null || flag == null || code == null) return result
    /** kids: without location_code */
    if (path.contains("kids_home")) {
      PAGE_ENTRANCE_KIDS_REGEX findFirstMatchIn path match {
        case Some(p) => {
          page_code = p.group(1).split("_")(0)
          area_code = p.group(2)
        }
        case None =>
      }
      area_code match {
        case "xuezhishi" => area_code = "kids_knowledge"
        case "tingerge" => area_code = "show_kidsSongSite"
        case "kandonghua" => area_code = "show_kidsSite"
        case "kids_anim" => area_code = "show_kidsSite"
        case "kids_rhymes" => area_code = "show_kidsSongSite"
        case "kids_songhome" => area_code = "show_kidsSongSite"
        case "kids_seecartoon" => area_code = "show_kidsSite"
        case "kids_recommend" => area_code = "kids_recommend"
        case "kids_cathouse" => area_code = "kids_collect"
        case "kids_collect" => area_code = "kids_collect"
        case _ =>
      }
    }

    /** detail 新版详情页**/
    if(pathSub != null && pathSub != "" && contentType != null && contentType != "" && pathSub.split("-")(2).split("\\*").length >=1 && pathSub.contains("\\*") && pathSub.contains("-") && pathSub.split("-").length>=3){
      page_code = "programPosition"+pathSub.split("-")(2).split("\\*")(1).substring(0,1).toUpperCase+pathSub.split("-")(2).split("\\*")(1).substring(1)
      area_code = pathSub.split("-")(0)
    }

    /** interest: interest-interest和interest-home都表示奇趣首页**/
    if(path.contains("interest-interest") || path.contains("interest-home")){
      PAGE_ENTRANGE_INTEREST_REGEX findFirstMatchIn path match {
        case Some(p) => {
          page_code = p.group(1).split("-")(0)
          if(p.group(2).contains("*")){
            area_code = p.group(2).split("[*]")(0)
          }
          else{
            area_code = p.group(2)
          }
        }
        case None =>
      }
    }

    /** hot: hot-hot和hot-home都表示资讯首页**/
    if(path.contains("hot-hot") || path.contains("hot-home")){
      PAGE_ENTRANGE_HOT_REGEX findFirstMatchIn path match {
        case Some(p) => {
          page_code = p.group(1).split("-")(0)
          if(p.group(2).contains("*")){
            area_code = p.group(2).split("[*]")(0)
          }
          else{
            area_code = p.group(2)
          }
        }
        case None =>
      }
    }

    /** game: game-game表示游戏首页 **/
    if (path.contains("game-game")) {
      PAGE_ENTRANGE_GAME_REGEX findFirstMatchIn path match {
        case Some(p) => {
          page_code = p.group(1).split("-")(0)
          if (p.group(2).contains("*")) {
            area_code = p.group(2).split("[*]")(0)
          }
          else {
            area_code = p.group(2)
          }
          if (path.contains("lp_gaming_site") || path.contains("lp_gaming_match") || path.contains("lp_gaming_subject_rec")) {
            location_code = p.group(3)
          }
        }
        case None =>
      }
    }

    /** mv: mvRecommendHomePage,mvTopHomePage一部分没有location_code */
    if (path.contains("mv*")) {
      /** home*classification*mv-mv*mvRecommendHomePage*8qlmwxd3abnp-mv_station */
      if (path.contains("mv_station") && path.contains("mvRecommendHomePage")) {
        page_code = "mv"
        area_code = "mvRecommendHomePage"
        location_code = "mv_station"
      } else {
        PAGE_ENTRANCE_MV_REGEX findFirstMatchIn path match {
          case Some(p) => {
            page_code = p.group(1)
            area_code = p.group(2)
            location_code = p.group(3)
          }
          case None =>
        }
        location_code match {
          case "rege_" => location_code = location_code.split("_")(0)
          case "xinge_" => location_code = location_code.split("_")(0)
          case "biaoshen_" => location_code = location_code.split("_")(0)
          case _ =>
        }
        if (!PAGE_ENTRANCE_LOCATION_CODE_LIST.contains(location_code)) location_code = null
      }
    }
    /** sports: without location_code */
    if (path.contains("sports")) {
      PAGE_ENTRANCE_SPORTS_REGEX findFirstMatchIn path match {
        case Some(p) => {
          page_code = p.group(1)
          area_code = p.group(2)
        }
        case None =>
      }
    }

    code match {
      case "page" => result = page_code
      case "area" => result = area_code
      case "location" => result = location_code
    }

    result
  }

  /** 通过area_code和location_code取得page_entrance_sk */
  def getPageEntranceSK(): DimensionColumn = {
    new DimensionColumn("dim_medusa_page_entrance",
      List(
        DimensionJoinCondition(
          /** location_code is not null,join with page_code, area_code and location_code. */
          Map("pageEntrancePageCode" -> "page_code", "pageEntranceAreaCode" -> "area_code", "pageEntranceLocationCode" -> "location_code"),
          " location_code is not null", null, " pageEntranceLocationCode is not null"
        ),
        DimensionJoinCondition(
          /** location_code is null,join with page_code and area_code. */
          Map("pageEntrancePageCode" -> "page_code", "pageEntranceAreaCode" -> "area_code"),
          " location_code = ''", null, " pageEntranceLocationCode is null"
        )
      ),
      "page_entrance_sk")
  }

  /**
    * 问题:
    * 1.解析出mvTopHomePage---personal_recommend在维度表没有对应记录
    *   home*classification*mv-mv*mvTopHomePage*personal_recommend
    * 2.mvCategoryHomePage---null在维度表没有对应记录,mineHomePage---null在维度表没有对应记录
    *   home*classification*mv-mv*mvCategoryHomePage*efv012a123b2
    *   home*classification*mv-mv*mineHomePage*2do8wxm7xz8q
    * 3.解析出horizontal---site_collect的记录
    *   home*my_tv*mv-mv*horizontal*site_collect-mv_collection
    * 4.home*classification*kids-kids_rhymes*舞蹈律动
    *   home*classification*kids-kids_home-kids_rhymes-search*SHUYA
    *
    *   area_code            location_code
    *   mvTopHomePage        personal_recommend     个人推荐
    *   horizontal(水平)      site_collect           收藏
    *   (现在添加下面两个记录)
    *   mvCategoryHomePage   location_code is null  其他分类
    *   mineHomePage         location_code is null  其他我的
    */

}
