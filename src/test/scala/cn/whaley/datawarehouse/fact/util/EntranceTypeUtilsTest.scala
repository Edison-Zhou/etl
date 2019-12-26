package cn.whaley.datawarehouse.fact.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.Assert._
import org.junit.{Before, Test}

/**
  * Created by michael on 2017/5/4.
  */
class EntranceTypeUtilsTest {
  var MORETV:String=_
  var MEDUSA:String=_
 // var path_4x: Seq[Row] = null

  @Before
  def initialize() {
    MORETV = "moretv"
    MEDUSA = "medusa"
//    val config = new SparkConf()
//    config.setMaster("local[2]")
//    val spark = SparkSession.builder().config(config).getOrCreate()
//    val rdd = spark.sparkContext.textFile("D:\\spark_test_log.txt")
//    path_4x = spark.read.json(rdd).
//      collect()(0).
//      getAs[Seq[Row]](0)
  }

  @Test
  def getEntranceCodeByPathETLTest: Unit ={
    val my_tv_medusa_list=List(
      ("home*my_tv*movie-movie*好莱坞巨制","xxx",MEDUSA,"my_tv","movie"),
      ("home*my_tv*tv-tv-search*WSZZ","xxx",MEDUSA,"my_tv","tv"),
      ("home*my_tv*movie-movie-retrieval*hot*dongzuo*gangtai*2013","xxx",MEDUSA,"my_tv","movie"),
      ("home*my_tv*kids-kids_home-kandonghua-search*SLATM","xxx",MEDUSA,"my_tv","kids"),
      ("home*my_tv*7-sports*recommend*tvn8aco8wx9v","xxx",MEDUSA,"my_tv","sports"),
      ("home*my_tv*7-sports*League*dzjj-league*英雄联盟","xxx",MEDUSA,"my_tv","sports"),
      ("home*my_tv*mv-mv*mineHomePage*site_collect-mv_collection","xxx",MEDUSA,"my_tv","mv"),
      ("home*my_tv*mv-mv*function*site_hotsinger-mv_poster","xxx",MEDUSA,"my_tv","mv")
    )

    val foundation_medusa_list=List(
      ("home*foundation*top_new-rank*top_new","xxx",MEDUSA,"foundation","top_new"),
      ("home*foundation*top_new","xxx",MEDUSA,"foundation","top_new"),
      ("home*foundation*top_star-rank*top_star","xxx",MEDUSA,"foundation","top_star"),
      ("home*foundation*interest_location","xxx",MEDUSA,"foundation","interest_location"),
      ("home*foundation*top_collect-rank*top_collect","xxx",MEDUSA,"foundation","top_collect"),
      ("home*foundation*interest_location-everyone_watching-everyone_nearby","xxx",MEDUSA,"foundation","interest_location"),
      ("home*foundation*top_star","xxx",MEDUSA,"foundation","top_star"),
      ("home*foundation*top_hot-rank*top_hot","xxx",MEDUSA,"foundation","top_hot"),
      ("home*foundation*interest_location-everyone_watching","xxx",MEDUSA,"foundation","interest_location")
    )

    val classification_medusa_list=List(
      ("home*classification*mv-mv*function*site_dance-mv_category*舞蹈教程","xxx",MEDUSA,"classification","mv"),
      ("home*classification*kids-kids_home-kandonghua-search*XSL","xxx",MEDUSA,"classification","kids"),
      ("home*classification*tv-tv-retrieval*hot*all*oumei*2017","xxx",MEDUSA,"classification","tv"),
      ("home*classification*zongyi-zongyi-search*ER ","xxx",MEDUSA,"classification","zongyi"),
      ("home*classification*3-sports*League*dzjj-league*王者荣耀","xxx",MEDUSA,"classification","sports"),
      ("home*classification*3-sports*horizontal*tvn8ackmikab","xxx",MEDUSA,"classification","sports")
    )

    val live_medusa_list=List(
      ("home*live*z206","xxx",MEDUSA,"live",null),
      ("home*live*eagle-movie-retrieval*hot*dongzuo*all*2016","xxx",MEDUSA,"live",null),
      ("home*live*eagle-kids_home-tingerge*随便听听","xxx",MEDUSA,"live",null),
      ("home*live*eagle-movie*犀利动作","xxx",MEDUSA,"live",null),
      ("home*live*s9n8opxyfhbc","xxx",MEDUSA,"live",null)
    )

    val recommendation_medusa_list=List(
      ("home*recommendation*1-hot*今日焦点","xxx",MEDUSA,"recommendation",null),
      ("home*recommendation*1-hot-hot","xxx",MEDUSA,"recommendation",null),
      ("home*recommendation*1-hot-search*YRYJZ","xxx",MEDUSA,"recommendation",null),
      ("home*recommendation*2","xxx",MEDUSA,"recommendation",null),
      ("home*recommendation*1-hot-hot-hot-hot*新闻热点","xxx",MEDUSA,"recommendation",null)
    )

    val search_medusa_list=List(
      ("home-search*DZL","xxx",MEDUSA,"search",null),
      ("tv-search*JUEDA","xxx",MEDUSA,"search",null),
      ("search*BP","xxx",MEDUSA,"search",null)
    )

    val setting_medusa_list=List(
      ("home*navi*setting-setmain-setaccount","xxx",MEDUSA,"setting",null),
      ("home*navi*setting-setmain","xxx",MEDUSA,"setting",null)
    )

    val live_moretv_list=List(
      ("xxx","home-live-1-s9n8opxyv0x0-jingpin-similar",MORETV,"live",null),
      ("xxx","home-live-1-12oq12k7a23e-jingpin",MORETV,"live",null),
      ("xxx","home-TVlive-1-s9n8op9wnpwx-jingpin",MORETV,"live",null),
      ("xxx","home-search-BXJC",MORETV,"search",null)
    )

    val more_moretv_list=List(
      ("xxx","home-history-subjectcollect-subject-comic13",MORETV,"my_tv","history"),
      ("xxx","home-hotrecommend-3-0 ",MORETV,"recommendation",null)
    )

    val more_case_list=List(
      ("home*my_tv*account-accountcenter_home*节目预约","xxx",MEDUSA,"my_tv","account"),
      ("home*my_tv*account","xxx",MEDUSA,"my_tv","account"),
      ("home*my_tv*comic-comic-retrieval*new*qita*neidi*2011","xxx",MEDUSA,"my_tv","comic"),
      ("home*my_tv*comic-comic-search*TMM","xxx",MEDUSA,"my_tv","comic"),
      ("home*my_tv*tv-comic-search*Q","xxx",MEDUSA,"my_tv","tv"),
      ("home*my_tv*zongyi-zongyi-search*XXZ","xxx",MEDUSA,"my_tv","zongyi"),
      ("home*my_tv*zongyi-zongyi-retrieval*hot*all*all*2016","xxx",MEDUSA,"my_tv","zongyi"),
      ("home*my_tv*mv-mv-search*ADM","xxx",MEDUSA,"my_tv","mv"),
      ("home*my_tv*mv-mv*mvCategoryHomePage*site_mvyear-mv_category*其他","xxx",MEDUSA,"my_tv","mv"),
      ("home*my_tv*mv-mv-retrieval*hot*landiao*all*all","xxx",MEDUSA,"my_tv","mv"),
      ("home*my_tv*mv-mv*mvTopHomePage*site_mvtop-mv_poster","xxx",MEDUSA,"my_tv","mv"),
      ("home*my_tv*mv-mv*function*site_mvsubject-mv_poster","xxx",MEDUSA,"my_tv","mv"),
      ("home*my_tv*mv-mv*horizontal*site_mvstyle-mv_category","xxx",MEDUSA,"my_tv","mv"),
      ("home*my_tv*movie-mv*mvRecommendHomePage*7oqrac3fij7o","xxx",MEDUSA,"my_tv","movie"),
      ("home*my_tv*movie-movie*好莱坞巨制","xxx",MEDUSA,"my_tv","movie"),
      ("home*my_tv*movie-movie-search*JIQIRENZ","xxx",MEDUSA,"my_tv","movie"),
      ("home*my_tv*movie-movie-retrieval*hot*all*gangtai*2014","xxx",MEDUSA,"my_tv","movie"),
      ("home*my_tv*tv-tv-search*KXWF","xxx",MEDUSA,"my_tv","tv"),
      ("home*my_tv*tv-tv-retrieval*new*wuxia*neidi*all","xxx",MEDUSA,"my_tv","tv"),
      ("home*my_tv*tv-tv*韩剧热流","xxx",MEDUSA,"my_tv","tv"),
      ("home*my_tv*kids-kids_home-kids_anim-search*DLZ","xxx",MEDUSA,"my_tv","kids"),
      ("home*my_tv*kids-kids_collect*观看历史","xxx",MEDUSA,"my_tv","kids"),
      ("home*my_tv*7-kids_home-tingerge*随便听听","xxx",MEDUSA,"my_tv","kids_home"),
      ("home*my_tv*kids","xxx",MEDUSA,"my_tv","kids"),
      ("home*my_tv*xiqu-xiqu*河北梆子","xxx",MEDUSA,"my_tv","xiqu"),
      ("home*my_tv*xiqu-xiqu-search*WDDXL","xxx",MEDUSA,"my_tv","xiqu"),
      ("home*my_tv*xiqu-xiqu*搜索","xxx",MEDUSA,"my_tv","xiqu"),
      ("home*my_tv*zongyi-xiqu*广场舞","xxx",MEDUSA,"my_tv","zongyi"),
      ("home*my_tv*7-sports*recommend*sports101","xxx",MEDUSA,"my_tv","sports"),
      ("home*my_tv*5-sports*League*guangchangwu-league*广场舞精选","xxx",MEDUSA,"my_tv","sports"),
      ("home*my_tv*movie-sports*League*dzjj-league*王者荣耀","xxx",MEDUSA,"my_tv","movie"),
      ("home*my_tv*hot-hot-search*WDSJFKX","xxx",MEDUSA,"my_tv","hot"),
      ("home*my_tv*hot-hot*生活时尚","xxx",MEDUSA,"my_tv","hot"),
      ("home*my_tv*7-history","xxx",MEDUSA,"my_tv","history"),
      ("home*my_tv*history-history","xxx",MEDUSA,"my_tv","history"),
      ("home*my_tv*jilu-jilu-retrieval*hot*all*all*2017","xxx",MEDUSA,"my_tv","jilu"),
      ("home*my_tv*jilu-jilu-search*HPZG","xxx",MEDUSA,"my_tv","jilu"),
      ("home*my_tv*jilu-jilu*历史钩沉","xxx",MEDUSA,"my_tv","jilu"),
      ("home*my_tv*7-jilu*纪实热播","xxx",MEDUSA,"my_tv","jilu"),
      ("home*my_tv*collect*收藏追看","xxx",MEDUSA,"my_tv","collect"),
      ("home*my_tv*collect-accountcenter_home*专题收藏-hot","xxx",MEDUSA,"my_tv","collect"),
      ("home*my_tv*live","xxx",MEDUSA,"my_tv","live"),
      ("home*my_tv*live-movie*院线大片","xxx",MEDUSA,"my_tv","live"),
      ("home*my_tv*live-history","xxx",MEDUSA,"my_tv","live"),

      ("home*recommendation*6","xxx",MEDUSA,"recommendation",null),
      ("home*recommendation*1-hot*轻松搞笑*电台","xxx",MEDUSA,"recommendation",null),
      ("home*recommendation*hot*新闻专题","xxx",MEDUSA,"recommendation",null),
      ("home*recommendation*sport","xxx",MEDUSA,"recommendation",null),

      ("home-search*HLX","xxx",MEDUSA,"search",null),
      ("home*navi*setting","xxx",MEDUSA,"setting",null),
      ("home*navi*setting-setmain","xxx",MEDUSA,"setting",null),
      ("home*navi*setting-setmain-setupdate","xxx",MEDUSA,"setting",null),
      ("home*live*eagle-hot*新闻专题","xxx",MEDUSA,"live",null),
      ("home*my_tv*live","xxx",MEDUSA,"my_tv","live"),
      ("home*live*lmobilegames-webcast*英雄联盟*电台","xxx",MEDUSA,"live",null),

      ("home*classification*xiqu-xiqu-search*MNQG","xxx",MEDUSA,"classification","xiqu"),
      ("home*classification*xiqu-xiqu*吕剧","xxx",MEDUSA,"classification","xiqu"),
      ("home*classification*xiqu","xxx",MEDUSA,"classification","xiqu"),
      ("home*classification*xiqu*潮剧","xxx",MEDUSA,"classification","xiqu"),
      ("home*classification*3-sports*League*ouguan-league*赛事回顾","xxx",MEDUSA,"classification","sports"),
      ("home*classification*3-sports","xxx",MEDUSA,"classification","sports"),
      ("home*classification*zongyi-zongyi-search*HDXS","xxx",MEDUSA,"classification","zongyi"),
      ("home*classification*zongyi-zongyi-retrieval*hot*gewu*taiwan*2011","xxx",MEDUSA,"classification","zongyi"),
      ("home*classification*zongyi-zongyi*综艺热播*电台","xxx",MEDUSA,"classification","zongyi"),
      ("home*classification*jilu-jilu-retrieval*hot*shehui*all*all","xxx",MEDUSA,"classification","jilu"),
      ("home*classification*jilu-jilu-search*JQ","xxx",MEDUSA,"classification","jilu"),
      ("home*classification*jilu-jilu*王牌栏目","xxx",MEDUSA,"classification","jilu")
    )

    val testCaseList=my_tv_medusa_list++foundation_medusa_list++classification_medusa_list++
      live_medusa_list++recommendation_medusa_list++search_medusa_list++setting_medusa_list++live_moretv_list++
      more_moretv_list++more_case_list
    //println("首页入口维度工具类测试用例")
    val medusa4xCaseList =List(
      ("","","medusa4x","editor_my","editor_my2","subjectCollection")
    )

    medusa4xCaseList.foreach(w => {
      //println(w._1+","+w._2+","+w._3+","+w._4+","+w._5)
//      val tableCode = EntranceTypeUtils.getEntranceTableCode(path_4x)
//      val areaCode=EntranceTypeUtils.getEntranceAreaCode(w._1,w._2,w._3)
//      val elementCode = EntranceTypeUtils.getEntranceElementCode(path_4x)
//      val locationCode = EntranceTypeUtils.getEntranceLocationCode(w._1,w._2,path_4x,w._3)
 //     assertEquals(w._4,areaCode)
//      assertEquals(w._4,tableCode)
//      assertEquals(w._5,elementCode)
//      assertEquals(w._6,locationCode)
    })
  }

}
