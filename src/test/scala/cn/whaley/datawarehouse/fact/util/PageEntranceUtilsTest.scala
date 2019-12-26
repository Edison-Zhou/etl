package cn.whaley.datawarehouse.fact.util

import org.junit.Assert._
import org.junit.{Before, Test}

/**
  * Created by wujiulin on 2017/5/4.
  */
class PageEntranceUtilsTest {

  var MORETV: String = _
  var MEDUSA: String = _

  @Before
  def initialize() {
    MORETV = "moretv"
    MEDUSA = "medusa"
  }

  @Test
  def getPageEntranceCode: Unit = {
    var path_message: String = null
    /** kids have moretv log and medusa log,mv and sports only have medusa log */
    val moretvTestCaseList = List(//pathMain, path, pathSub, contentType, flag, page_code, area_code, location_code
      ("", "home-kids_home-kids_songhome-search-CTZ", "","",MORETV, "kids", "show_kidsSongSite", null),
      ("", "thirdparty_0-kids_home-kids_seecartoon-search-KAIL","","", MORETV, "kids", "show_kidsSite", null),
      ("", "home-kids_home-kids_recommend-kids192", "","",MORETV, "kids", "kids_recommend", null),
      ("", "home-kids_home-kids_cathouse-%E7%AB%A5%E5%B9%B4","","", MORETV, "kids", "kids_collect", null)
    )

    val medusaTestCaseList = List(//pathMain, path, flag, page_code, area_code, location_code
      //kids
      ("home*classification*kids-kids_home-kandonghua-search*PPH", "","","",MEDUSA, "kids", "show_kidsSite", null),
      ("home*classification*kids-kids_home-tingerge-search*HLW", "", "","",MEDUSA, "kids", "show_kidsSongSite", null),
      ("home*classification*kids-kids_home-xuezhishi-search*LBY", "", "","",MEDUSA, "kids", "kids_knowledge", null),
      ("kids_home-kids_anim*国产精选", "","","", MEDUSA, "kids", "show_kidsSite", null),
      ("home-kids_home-kids_rhymes*单曲精选", "","","", MEDUSA, "kids", "show_kidsSongSite", null),
      ("home*my_tv*kids-kids_home-kids_collect*观看历史", "","","", MEDUSA, "kids", "kids_collect", null),

      //interest
      ("home*classification*interest-interest*分类入口*6-interest*收藏", "", "","",MEDUSA, "interest", "分类入口", null),
      ("home*classification*interest-interest*分类入口*3-interest-search*JXY", "", "","",MEDUSA, "interest", "分类入口", null),
      ("home*my_tv*interest-interest*分类入口*6-interest*全网热门", "", "","",MEDUSA, "interest", "分类入口", null),
      ("home*classification*interest-interest*专题推荐区*1-column*【综合】绝不能错过*4", "", "","",MEDUSA, "interest", "专题推荐区", null),
      ("home*classification*interest-interest*分类入口*4-interest*收藏", "","","", MEDUSA, "interest", "分类入口", null),
      ("home*classification*interest-interest*分类入口*4-interest-search*QQD", "","","", MEDUSA, "interest", "分类入口", null),
      ("home*my_tv*interest-interest*热词区", "", "","",MEDUSA, "interest", "热词区", null),
      ("home*my_tv*interest-interest*热词区*2", "", "","",MEDUSA, "interest", "热词区", null),
      ("home*my_tv*interest-interest*分类入口*3-interest*文艺咖", "", "","",MEDUSA, "interest", "分类入口", null),
      ("home*classification*interest-interest*分类入口*4-interest*创意运动", "", "","",MEDUSA, "interest", "分类入口", null),
      ("home*classification*interest-interest*专题推荐区*1-column*【综合】绝不能错过*4", "", "","",MEDUSA, "interest", "专题推荐区", null),
      ("home*classification*interest-interest*分类入口*5-interest*游戏动画*电台", "", "","",MEDUSA, "interest", "分类入口", null),
      ("home*my_tv*interest-interest*单片人工推荐区*10-interest", "","","", MEDUSA, "interest", "单片人工推荐区", null),
      ("home*classification*interest-home*单片人工推荐区*2", "","","", MEDUSA, "interest", "单片人工推荐区", null),
      ("home*classification*interest-interest*首屏专题推荐区*3", "", "","",MEDUSA, "interest", "首屏专题推荐区", null),

      //hot
      ("home*classification*hot-hot*单片人工推荐区*26", "", "","",MEDUSA, "hot", "单片人工推荐区", null),

      //detail
      ("home*classification*movie-movie*mvRecommendHomePage*2dpr34nowy5g","","similar-tvwykmmoa18r*movie-tvwyqsklqtf5*movie", "movie", MEDUSA, "programPositionMovie", "similar", null),

      //mv
      ("home*classification*mv-mv*mvRecommendHomePage*8qc3op34qr23-mv_station", "", "","",MEDUSA, "mv", "mvRecommendHomePage", "mv_station"),
      ("home*classification*mv-mv*mvRecommendHomePage*2dpr34nowy5g", "", "","",MEDUSA, "mv", "mvRecommendHomePage", null),
      ("home*my_tv*mv-mv*mvRecommendHomePage*personal_recommend", "", "","",MEDUSA, "mv", "mvRecommendHomePage", "personal_recommend"),

      ("mv*function*site_dance-mv_category*宅舞", "", "","",MEDUSA, "mv", "function", "site_dance"),
      ("home*classification*mv-mv*function*site_concert-mv_category*华语", "", "","",MEDUSA, "mv", "function", "site_concert"),
      ("mv*function*site_hotsinger-mv_poster", "", "","",MEDUSA, "mv", "function", "site_hotsinger"),
      ("home*my_tv*mv-mv*function*site_mvsubject-mv_poster", "", "","",MEDUSA, "mv", "function", "site_mvsubject"),

      ("home*classification*mv-mv*mvTopHomePage*xinge_2017_92", "", "","",MEDUSA, "mv", "mvTopHomePage", "xinge"),
      ("home*classification*mv-mv*mvTopHomePage*biaoshen_2017_71", "", "","",MEDUSA, "mv", "mvTopHomePage", "biaoshen"),
      ("home*classification*mv-mv*mvTopHomePage*rege_2017_76", "","","", MEDUSA, "mv", "mvTopHomePage", "rege"),
      ("home*classification*mv-mv*mvTopHomePage*o83est3f5gkm", "", "","",MEDUSA, "mv", "mvTopHomePage", null),

      ("home*classification*mv-mv*mvCategoryHomePage*site_mvarea-mv_category*内地", "", "","",MEDUSA, "mv", "mvCategoryHomePage", "site_mvarea"),
      ("home*my_tv*mv-mv*mvCategoryHomePage*site_mvyear-mv_category*其他", "", "","",MEDUSA, "mv", "mvCategoryHomePage", "site_mvyear"),
      ("home*my_tv*mv-mv*mvCategoryHomePage*site_mvstyle-mv_category*电子", "","","", MEDUSA, "mv", "mvCategoryHomePage", "site_mvstyle"),
      ("home*classification*mv-mv*mvCategoryHomePage*efv012a123b2", "", "","",MEDUSA, "mv", "mvCategoryHomePage", null),

      ("home*my_tv*mv-mv*mineHomePage*site_collect-mv_collection", "", "","",MEDUSA, "mv", "mineHomePage", "site_collect"),
      ("home*classification*mv-mv*mineHomePage*2do8wxm7xz8q", "", "","",MEDUSA, "mv", "mineHomePage", null),
      //sports
      ("sports*newsHomePage*tvn8xz1b8qab", "", "","",MEDUSA, "sports", "newsHomePage", null),
      ("home*classification*3-sports*newsHomePage*tvn8iklni6vw", "", "","",MEDUSA, "sports", "newsHomePage", null),
      ("home*classification*3-sports*welfareHomePage*tuqsd3n8rtv0", "", "","",MEDUSA, "sports", "welfareHomePage", null),
      ("sports*welfareHomePage*tuqsd3n8rtv0", "", "","",MEDUSA, "sports", "welfareHomePage", null),
      ("home*my_tv*4-sports*recommend*x0d3demnln5h", "", "","",MEDUSA, "sports", "recommend", null),
      ("sports*recommend*x0d3demnln5h-zongyi*recommend*x0d3demnln5h-zong", "", "","",MEDUSA, "sports", "recommend", null),
      ("home*my_tv*7-sports*collect*collect-sportcollection*比赛", "", "","",MEDUSA, "sports", "collect", null),
      ("home*classification*3-sports*collect*collect-sportcollection*视频", "", "","",MEDUSA, "sports", "collect", null),
      ("sports*League*dzjj-league*王者荣耀", "","","", MEDUSA, "sports", "League", null),
      ("home*my_tv*5-sports*League*dzjj-league", "", "","",MEDUSA, "sports", "League", null)
    )
//    val testCaseList = moretvTestCaseList ++ medusaTestCaseList
//    testCaseList.foreach(f = w => {
//      if (w._1 == "") path_message = w._2 else path_message = w._1
//      val page_code = PageEntrancePathParseUtils.getPageEntrancePageCode(w._1, w._2, w._3,w._4,w._5)
//      val area_code = PageEntrancePathParseUtils.getPageEntranceAreaCode(w._1, w._2, w._3,w._4,w._5)
//      val location_code = PageEntrancePathParseUtils.getPageEntranceLocationCode(w._1, w._2, w._3,w._4,w._5)
//      try {
//        assertEquals(w._6, page_code)
//        assertEquals(w._7, area_code)
//        assertEquals(w._8, location_code)
//      } catch {
//        case e: AssertionError =>
//          println("fail test case: " + path_message)
//          throw e
//      }
//
//    })
  }

}
