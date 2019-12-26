package cn.whaley.datawarehouse.fact.moretv.util

import java.sql.Struct

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}
import cn.whaley.datawarehouse.fact.constant.UDFConstantDimension
import cn.whaley.datawarehouse.fact.moretv.util.EntranceTypeUtils.MEDUSA4X
import cn.whaley.datawarehouse.global.{DimensionTypes, LogConfig}
import com.alibaba.fastjson.JSON
import org.apache.avro.TestAnnotation
import org.apache.spark.sql.Row

import scala.util.control.Breaks.{break, breakable}

/**
  * Created by baozhiwang on 2017/4/24.
  * Updated by wujiulin on 2017/5/10.
  */
object ListCategoryUtils extends LogConfig {

  private val MEDUSA_LIST_PAGE_LEVEL_1_REGEX = UDFConstantDimension.MEDUSA_LIST_Page_LEVEL_1.mkString("|")
  private val regex_medusa_list_category_other = (s".*($MEDUSA_LIST_PAGE_LEVEL_1_REGEX)\\*([a-zA-Z0-9&\u4e00-\u9fa5]+)").r
  private val regex_medusa_list_retrieval = (s".*($MEDUSA_LIST_PAGE_LEVEL_1_REGEX)[-*]?(${UDFConstantDimension.RETRIEVAL_DIMENSION}|${UDFConstantDimension.RETRIEVAL_DIMENSION_CHINESE}).*").r
  private val regex_medusa_list_search = (s".*($MEDUSA_LIST_PAGE_LEVEL_1_REGEX)[-*]?(${UDFConstantDimension.SEARCH_DIMENSION}|${UDFConstantDimension.SEARCH_DIMENSION_CHINESE}).*").r
  /** 用于频道分类入口统计，解析出资讯的一级入口、二级入口 */
  private val regex_medusa_recommendation = (s"home\\*recommendation\\*[\\d]{1}-($MEDUSA_LIST_PAGE_LEVEL_1_REGEX)\\*(.*)").r
  private val MoretvLauncherAccessLocation = Array("search","setting","history","movie","tv","live","hot","zongyi","comic","mv",
    "jilu","xiqu","sports","kids_home","subject")
  private   val MoretvPageInfo = Array("history","movie","tv","zongyi","hot","comic","mv","xiqu","sports","jilu","subject","live",
    "search","kids_home")

  private val MoretvLauncherUPPART = Array("watchhistory","otherswatch","hotrecommend","TVlive")


  // TODO
  def getListMainCategory(pathMain: String, path: String, path_json: Seq[Row], flag: String): String = {
    var result: String = null
    flag match {
      case MEDUSA => {
        result = getListCategoryMedusaETL(pathMain, 1)
      }
      case MORETV => {
        result = getListCategoryMoretvETL(path, 1)
      }
      case MEDUSA4X => {
        result = getListCategoryMedusa4xETL(path_json, 1)
      }
      case UTVMORE => { //优视猫的路径和电视猫4x是一致的，所以这里采取一样的逻辑
        result = getListCategoryMedusa4xETL(path_json, 1)
      }
    }
    result
  }

  // for main3x
  def getListMainCategory(pathMain: String, path: String, flag: String): String = {
    getListMainCategory(pathMain,path,null,flag)
  }

  def getListSecondCategory(pathMain: String, path: String, path_json: Seq[Row], flag: String): String = {
    var result: String = null
    flag match {
      case MEDUSA => {
        result = getListCategoryMedusaETL(pathMain, 2)
      }
      case MORETV => {
        result = getListCategoryMoretvETL(path, 2)
      }
      case MEDUSA4X => {
        result = getListCategoryMedusa4xETL(path_json, 2)
      }
      case UTVMORE => { //优视猫的路径和电视猫4x是一致的，所以这里采取一样的逻辑
        result = getListCategoryMedusa4xETL(path_json, 2)
      }
    }
    result
  }

  //for main3x
  def getListSecondCategory(pathMain: String, path: String,flag: String): String = {
    getListSecondCategory(pathMain,path,null,flag)
  }

  def getListThirdCategory(pathMain: String, path: String, path_json: Seq[Row], flag: String): String = {
    var result: String = null
    flag match {
      case MEDUSA => {
        result = getListCategoryMedusaETL(pathMain, 3)
      }
      case MORETV => {
        result = getListCategoryMoretvETL(path, 3)
      }
      case MEDUSA4X => {
        result = getListCategoryMedusa4xETL(path_json, 3)
      }
      case UTVMORE => { //优视猫的路径和电视猫4x是一致的，所以这里采取一样的逻辑
        result = getListCategoryMedusa4xETL(path_json, 3)
      }
    }
    result
  }

  //for main3x
  def getListThirdCategory(pathMain: String, path: String,flag: String): String = {
    getListThirdCategory(pathMain,path,null,flag)
  }
  /** 解析列表页四级入口，针对sports 和 game */
  def getListFourthCategory(pathMain: String, path: String, path_json: Seq[Row], flag: String): String = {
    var result: String = null
    flag match {
      case MEDUSA => {
        if (pathMain == null || (!pathMain.contains(UDFConstantDimension.SPORTS_LIST_DIMENSION_TRAIT) && !pathMain.contains(UDFConstantDimension.GAME_GAME))) return result
        else
           result = getListCategoryMedusaETL(pathMain, 4)
      }
      case MEDUSA4X => {
        result = getListCategoryMedusa4xETL(path_json, 4)
      }
      case UTVMORE => { //优视猫的路径和电视猫4x是一致的，所以这里采取一样的逻辑
        result = getListCategoryMedusa4xETL(path_json, 4)
      }
      case MORETV => result = null
    }
    result
  }

  //for main3x
  def getListFourthCategory(pathMain: String, path: String,flag: String): String = {
    getListFourthCategory(pathMain,path,null,flag)
  }

  /**
    * 获取列表页入口信息
    * 第一步，过滤掉包含search字段的pathMain
    * 第二步，判别是来自classification还是来自my_tv
    * 第三步，分音乐、体育、少儿以及其他类型【电视剧，电影等】获得列表入口信息,根据具体的分类做正则表达
    */
  def getListCategoryMedusaETL(pathMain: String, index_input: Int): String = {
    var result: String = null
    if (null == pathMain || pathMain.contains(UDFConstantDimension.HOME_SEARCH)) {
      result = null
    }
      /** 少儿kids */
      else if (pathMain.contains(UDFConstantDimension.KIDS)) {
        result = KidsPathParserUtils.pathMainParse(pathMain, index_input)
        if(2 == index_input && null != result){
          result match {
            case "tingerge" => result="show_kidsSongSite"
            case "kids_rhymes" => result="show_kidsSongSite"
            case "kids_songhome" => result="show_kidsSongSite"
            case "kids_seecartoon" => result="show_kidsSite"
            case "kandonghua" => result="show_kidsSite"
            case "kids_anim" => result="show_kidsSite"
            case "xuezhishi" => result="kids_knowledge"
            case _ =>
          }
        }
      }
      /** 音乐mv */
      else if (pathMain.contains(UDFConstantDimension.MV_CATEGORY) || pathMain.contains(UDFConstantDimension.MV_POSTER)) {
        result = MvPathParseUtils.pathMainParse(pathMain,index_input)
      }

      /** 奇趣interest */
    else if (pathMain.contains(UDFConstantDimension.INTEREST_INTEREST) || pathMain.contains(UDFConstantDimension.INTEREST_HOME)) {
         result = InterestPathParseUtils.pathMainParse(pathMain,index_input)
    }
      /**资讯hot（与站点树相关部分，路径中带“分类入口”字样）*/
    else if (pathMain.contains(UDFConstantDimension.HOT_HOT)||pathMain.contains(UDFConstantDimension.HOT_HOME)){
      result = HotPathParseUtils.pathMainParse(pathMain,index_input)
    }

    /** 游戏game */
    else if (pathMain.contains(UDFConstantDimension.GAME_GAME)) {
      result = GamePathParseUtils.pathMainParse(pathMain, index_input)
    }

      /** 体育sports */
      else if (pathMain.contains(UDFConstantDimension.SPORTS_LIST_DIMENSION_TRAIT)) {
        result = SportsPathParserUtils.pathMainParse(pathMain, index_input)
        if(2 == index_input && "League".equalsIgnoreCase(result)){
          result = "leagueEntry"
        }
      }
      /** 筛选维度 */
      else if (pathMain.contains(UDFConstantDimension.RETRIEVAL_DIMENSION) || pathMain.contains(UDFConstantDimension.RETRIEVAL_DIMENSION_CHINESE)) {
        regex_medusa_list_retrieval findFirstMatchIn pathMain match {
          case Some(p) => {
            if (index_input == 1) {
              result = p.group(1)
            } else if (index_input == 2) {
              result = UDFConstantDimension.RETRIEVAL_DIMENSION_CHINESE
            }
          }
          case None =>
        }
      }
      /** 搜索维度 */
      else if (pathMain.contains(UDFConstantDimension.SEARCH_DIMENSION) || pathMain.contains(UDFConstantDimension.SEARCH_DIMENSION_CHINESE)) {
        regex_medusa_list_search findFirstMatchIn pathMain match {
          case Some(p) => {
            if (index_input == 1) {
              result = p.group(1)
            } else if (index_input == 2) {
              result = UDFConstantDimension.SEARCH_DIMENSION_CHINESE
            }
          }
          case None =>
        }
      }
      /** home*recommendation*1-hot*今日焦点 解析出 hot,今日焦点 */
      else if (pathMain.contains(UDFConstantDimension.HOME_RECOMMENDATION)) {
        regex_medusa_recommendation findFirstMatchIn pathMain match {
          case Some(p) => {
            if (index_input == 1) {
              result = p.group(1)
            } else if (index_input == 2) {
              result = p.group(2)
            }
          }
          case None =>
        }
      }

    else if ((pathMain.contains(UDFConstantDimension.ACCOUNTCENTER_HOME) || pathMain.contains(UDFConstantDimension.COLLECT)) && index_input == 1){
      result = "history_collect"
    }

      /** 其他频道,例如 电影，电视剧 */
//      else {
//        regex_medusa_list_category_other findFirstMatchIn pathMain match {
//          case Some(p) => {
//            if (index_input == 1) {
//              result = p.group(1)
//            } else if (index_input == 2) {
//              result = p.group(2)
//            }
//          }
//          case None =>
//        }
//      }
    //20181127發現站點樹3x層級關係未讀入，重新修改邏輯
    else{
      regex_medusa_list_category_other findFirstMatchIn pathMain match {
        case Some(p) => {
          if (index_input == 1) {
            result = p.group(1)
          }else if(index_input == 2){
            result = "site_" + p.group(1)
          }else if (index_input == 3){
            result = p.group(2)
          }
        }
        case None =>
      }
    }

    result
  }

  /**
    * 2.x，原有统计分析没有做少儿；体育最新的逻辑解析没有上线
    * SportsPathParserUtils现在没有解析2.x path路径
    **/
  def getListCategoryMoretvETL(path: String, index_input: Int): String = {
    var result: String = null
    //去除过滤包含"search"的逻辑, !path.contains(UDFConstantDimension.SEARCH_DIMENSION)
    if (null != path) {
      //少儿使用最新逻辑
      if (path.contains(UDFConstantDimension.KIDS)) {
        result = KidsPathParserUtils.pathParse(path, index_input)
        if(2 == index_input && null != result) {
          result match {
            case "tingerge" => result="show_kidsSongSite"
            case "kids_rhymes" => result="show_kidsSongSite"
            case "kids_songhome" => result="show_kidsSongSite"
            case "kids_cathouse" => result="show_kidsSongSite"
            case "kids_seecartoon" => result="show_kidsSite"
            case "kandonghua" => result="show_kidsSite"
            case "kids_anim" => result="show_kidsSite"
            case "xuezhishi" => result="kids_knowledge"
            case _ =>
          }
        }
      } else {
        //其他类型仍然使用原有逻辑
        if (index_input == 1) {
          result = PathParserUtils.getSplitInfo(path, 2)
          if (result != null) {
            // 如果accessArea为“navi”和“classification”，则保持不变，即在launcherAccessLocation中
            if (!MoretvLauncherAccessLocation.contains(result)) {
              // 如果不在launcherAccessLocation中，则判断accessArea是否在uppart中
              if (MoretvLauncherUPPART.contains(result)) {
                result = "MoretvLauncherUPPART"
              } else {
                result = null
              }
            }
          }
        } else if (index_input == 2) {
          result = PathParserUtils.getSplitInfo(path, 3)
          if (result != null) {
            if (PathParserUtils.getSplitInfo(path, 2) == "sports") {
              result = PathParserUtils.getSplitInfo(path, 3) + "-" + PathParserUtils.getSplitInfo(path, 4)
            }
            if (!MoretvPageInfo.contains(PathParserUtils.getSplitInfo(path, 2))) {
              result = null
            }
          }
        }
      }
    }
    result
  }


  def getSourceSiteSK() :DimensionColumn = {
    new DimensionColumn("dim_medusa_source_site",
      List(
        //获得MEDUSA中314以後的電影、電視劇、動漫、資訊短片、紀錄片、戲曲、綜藝、粵語、會員的列表维度sk ，[一級、二級、三級]
        DimensionJoinCondition(
          Map("secondCategory" -> "second_category_code","thirdCategory"->"third_category"),
          "site_content_type is not null and (main_category_code  = 'program_site' or main_category " +
            "in ('粤语频道站点树','会员频道站点树')) ",
          null,s" flag='$MEDUSA'"
        ),
        //获得MORETV中除了少儿，体育和音乐的列表维度sk ，[只有一级，二级维度]
        DimensionJoinCondition(
          Map("mainCategory" -> "site_content_type","secondCategory" -> "second_category_code"),
          "site_content_type is not null and main_category_code in " +
            "('site_tv','site_movie','site_xiqu','site_comic','site_zongyi','site_hot','site_jilu')",
          null,s" flag='$MORETV' and mainCategory not in ('$CHANNEL_SPORTS','$CHANNEL_KIDS','$CHANNEL_MV')"
        ),
        //获得音乐的列表维度sk ，热门歌手，精选集，电台，排行榜只到二级维度 [只有有一级，二级维度]
        DimensionJoinCondition(
          Map("mainCategory" -> "site_content_type","secondCategory" -> "second_category_code"),
          s"site_content_type in ('$CHANNEL_MV') and main_category_code in ('mv_site') and second_category_code in ('site_hotsinger','site_mvtop','site_mvradio','site_mvsubject')" ,
          null,s" mainCategory in ('$CHANNEL_MV') and secondCategory in ('site_hotsinger','site_mvtop','site_mvradio','site_mvsubject') "
        ),

        //获得medusa奇趣的列表维度sk ，main_categoty為奇趣 站点树1.0
        DimensionJoinCondition(
          Map("secondCategory" -> "second_category_code","thirdCategory"->"third_category"),
          s"main_category = '奇趣 站点树1.0'",null,s"flag='$MEDUSA' and mainCategory = '$CHANNEL_INTEREST'"
        ),

        //获得medusa游戏列表维度sk ，[只有一级，二级,三级维度]
        DimensionJoinCondition(
          Map("secondCategory" -> "second_category_code", "thirdCategory" -> "third_category"),
          s"main_category_code = 'gaming_site' and (fourth_category is null or fourth_category='' or fourth_category='null')",
          null, s" flag='$MEDUSA' and (fourthCategory is null or fourthCategory='' or fourthCategory='null')"
        ),

        //获得medusa游戏列表维度sk ，[有一级，二级,三级，四级维度]
        DimensionJoinCondition(
          Map("secondCategory" -> "second_category_code", "thirdCategory" -> "third_category_code", "fourthCategory" -> "fourth_category"),
          s"main_category_code = 'gaming_site' and fourth_category is not null and fourth_category<>'' and fourth_category<>'null'",
          null, s" flag='$MEDUSA' and fourthCategory is not null and fourthCategory<>'' and fourthCategory<>'null'"
        ),


        //moretv日志里的少儿维度，三级入口需要使用code关联, [有一级，二级,三级维度]
        DimensionJoinCondition(
          Map("mainCategory" -> "site_content_type","secondCategory" -> "second_category_code","thirdCategory"->"third_category_code"),
          s"site_content_type in ('$CHANNEL_KIDS') and main_category_code in " +
            "('kids_site')",
          null,s" flag='$MORETV' and mainCategory in ('$CHANNEL_KIDS')"
        ),

        //获得少儿和音乐的列表维度sk ，[有一级，二级,三级维度]
        DimensionJoinCondition(
          Map("secondCategory" -> "second_category_code","thirdCategory"->"third_category"),
          s"((main_category = '少儿站点树' and source_site_id >= 3200) or main_category = '音乐站点树') and main_category_code in " +
            "('kids_site','mv_site')",
          null,s" mainCategory in ('$CHANNEL_KIDS','$CHANNEL_MV')"
        ),
        //获得体育列表维度sk ，[有一级，二级,三级,四级维度]
        DimensionJoinCondition(
          Map("secondCategory" -> "second_category_code","thirdCategory"->"third_category_code","fourthCategory"->"fourth_category"),
          s" main_category_code = 'sportRootNew' and fourth_category is not null and fourth_category<>'' and fourth_category<>'null'",
          null,s" flag='$MEDUSA' and  fourthCategory is not null and fourthCategory<>'' and fourthCategory<>'null'"
        ),
        //获得体育列表维度sk ，[只有一级，二级,三级维度]
        DimensionJoinCondition(
          Map("secondCategory" -> "second_category_code","thirdCategory"->"third_category_code"),
          s" main_category_code = 'sportRootNew' and (fourth_category is null or fourth_category='' or fourth_category='null') ",
          null,s" flag='$MEDUSA' and  (fourthCategory is null or fourthCategory='' or fourthCategory='null')"
        ),
        //获得历史收藏列表维度sk，只映射mainCategory，目前只有攻坚指标剔除历史收藏数据这份需求
        DimensionJoinCondition(
          Map("mainCategory" -> "main_category_code"),
          s"main_category_code = 'history_collect'",
          null,s" mainCategory = 'history_collect'"
        )
      ),
      "source_site_sk")
  }

  def getSportsSecondCategory() :DimensionColumn = {
    //获得体育的列表页二级入口中文名称
    new DimensionColumn("dim_medusa_page_entrance",
      List(DimensionJoinCondition(
        Map("mainCategory" -> "page_code","secondCategory" -> "area_code"),
        s"page_code='$CHANNEL_SPORTS' ",
        null,s"mainCategory='$CHANNEL_SPORTS'"
      )),
      "page_entrance_sk")
  }

  def getListCategoryMedusa4xETL(path: Seq[Row],index:Int): String ={
    var result: String = null
    path.foreach(data => {
      val page_type = data.getAs[String]("page_type")
      val page_tab = data.getAs[String]("page_tab")

      //获取treeSite
      var treeSite: String = ""
      breakable{
        if (data.getAs[String]("page_url") != null) {
          data.getAs[String]("page_url").split("&").foreach(url => {
            val kvs = url.split("=")
            if (kvs(0) == "treeSite" && kvs.length > 1) {
              treeSite = kvs(1)
              break()
            }
          })
        }
      }


      //获取channelType
      var channelType: String = ""
      breakable{
        if (data.getAs[String]("page_url") != null) {
          data.getAs[String]("page_url").split("&").foreach(url => {
            val kvs = url.split("=")
            if (kvs(0) == "channelType" && kvs.length > 1) {
              channelType = kvs(1)
              break()
            }
          })
        }
      }

      /**
        * 通用频道 && 少儿 && 游戏体育 三级站点树
        */
      if ((page_type == "VodActivity" || page_type == "KidsAnimActivity" || page_type == "KidsRhymesActivity" || page_type == "KidsCollectActivity") && index == 1) {
        result = channelType + "_site"
      }
      else if ((page_type == "VodActivity" || page_type == "KidsAnimActivity" || page_type == "KidsRhymesActivity" || page_type == "KidsCollectActivity") && index == 2 && treeSite.contains('_')) {
        result = treeSite
      }
      else if ((page_type == "VodActivity" || page_type == "KidsAnimActivity" || page_type == "KidsRhymesActivity" || page_type == "KidsCollectActivity") && index == 3 && treeSite.contains('_')) {
        result = page_tab
      }
      /**
        * 游戏体育直播首页四级站点树（主要针对专区）
        */
      else if (page_type == "VodActivity" && index == 2 && treeSite != null && !treeSite.contains('_')){
        if(channelType == "game")
          result = "gameArea"
        else if (channelType == "sports")
          result = "sportsArea"
        else if (channelType == "webcast") //匹配直播首页的专区-热门专区
          result = "livegamecentre"
      }
      else if (page_type == "VodActivity" && index == 3 && treeSite != null && !treeSite.contains('_')){
         result = treeSite
      }
      else if (page_type == "VodActivity" && index == 4 && treeSite != null && !treeSite.contains('_')){
         result = page_tab
      }

      /**
        * 历史收藏站点树根节点解析
        * 记录：只解析根节点，不解析站点树tab，目前需要这个维度数据的是攻坚指标里需要排除历史收藏播放记录
        *       另一个涉及到的需求是各频道的播放入口，但针对该需求直接生成一个字段；
        *       维度表无法从cms拉到该站点树信息，只能手动写死，目前没有将站点树tab信息写死到维度表
        */
      else if(page_type == "UserCenterActivity" && index == 1){
        result = "history_collect"
      }

    })
    result
  }

  def c() :DimensionColumn = {
    new DimensionColumn(s"${DimensionTypes.DIM_MEDUSA_SOURCE_SITE}",
      List(DimensionJoinCondition(
        Map("subjectCode" -> "subject_code"),
        null,null,null
      ),
        DimensionJoinCondition(
          Map("subjectName" -> "subject_name"),
          null,null,null
        )
      ),
      "subject_sk")
  }

  @TestAnnotation
  def main(args: Array[String]): Unit = {
    val str = "home*classification*11-cantonese*热播港剧"
    val str1 = "home*hotSubject*5-interest"
    println(getListCategoryMedusaETL(str1, 2))
  //  println(getListMainCategory(str1, "",MEDUSA))
  }

}
