package cn.whaley.datawarehouse.fact.util

import org.junit.Assert.assertEquals
import org.junit.{Before, Test}

/**
  * Created by wujiulin on 2017/5/8.
  */
class RecommendUtilsTest {

  var MORETV: String = _
  var MEDUSA: String = _

  @Before
  def initialize() {
    MORETV = "moretv"
    MEDUSA = "medusa"
  }

  @Test
  def getRecommend: Unit = {
    var path_message: String = null
    val moretvTestCaseList = List(//pathSub, path, flag, recommendSourceType, previousSid, previousContentType, recommendSlotIndex
      ("", "thirdparty_0-tag-tag-tag-%E9%BB%91%E5%B8%AE-peoplealsolike", MORETV, "peoplealsolike", null, null, null),
      ("", "thirdparty_0-kids_home-kids_seecartoon-kids_star-kids98-similar", MORETV, "similar", null, null, null),
      ("", "home-movie-movie_zhuanti-movie266-peoplealsolike", MORETV, "peoplealsolike", null, null, null),
      ("", "home-kids_home-kids_seecartoon-kids_star-kids169-similar", MORETV, "similar", null, null, null),
      ("", "home-search-MS-peoplealsolike", MORETV, "peoplealsolike", null, null, null)
    )

    val medusaTestCaseList = List(//pathSub, path, flag, recommendSourceType, previousSid, previousContentType, recommendSlotIndex
      ("similar-vx2ch6i6qrbc*tv-5ivw4go8hjnp*tv", "", MEDUSA, "similar", "5ivw4go8hjnp", "tv", null),
      ("peoplealsolike-9wqtg6v01cbc*movie-s9n8p8wys93e*movie", "", MEDUSA, "peoplealsolike", "s9n8p8wys93e", "movie", null),
      ("guessyoulike-tvn8fhacfgx0*mv-tvn8fhac7nvw*mv", "", MEDUSA, "guessyoulike", "tvn8fhac7nvw", "mv", null),
      //recommendSlotIndex
      ("home*recommendation*10", "", MEDUSA, null, null, null, "10"),
      ("home*recommendation*13", "", MEDUSA, null, null, null, "13"),
      ("home*recommendation*1-hot*五花八门", "", MEDUSA, null, null, null, null),
      ("home*recommendation*1-hot-search*PN", "", MEDUSA, null, null, null, null),
      ("home*recommendation*1*电台", "", MEDUSA, null, null, null, null),
      ("home*recommendation*movie", "", MEDUSA, null, null, null, null)
    )

//    val testCaseList = moretvTestCaseList ++ medusaTestCaseList
//    testCaseList.foreach(f = w => {
//      if (w._1 == "") path_message = w._2 else path_message = w._1
//      val recommendSourceType = RecommendUtils.getRecommendSourceType(w._1, w._2, w._3)
//      val previousSid = RecommendUtils.getPreviousSid(w._1)
//      val previousContentType = RecommendUtils.getPreviousContentType(w._1)
//      val recommendSlotIndex = RecommendUtils.getRecommendSlotIndex(w._1)
//      try {
//        assertEquals(w._4, recommendSourceType)
//        assertEquals(w._5, previousSid)
//        assertEquals(w._6, previousContentType)
//        assertEquals(w._7, recommendSlotIndex)
//      } catch {
//        case e: AssertionError =>
//          println("fail test case: " + path_message)
//          throw e
//      }
//
//    })

  }
}
