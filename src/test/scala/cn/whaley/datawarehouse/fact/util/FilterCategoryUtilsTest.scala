package cn.whaley.datawarehouse.fact.util

import org.junit.Test
import org.junit.Assert._
import org.junit.Before
/**
  * Created by michael on 2017/5/3.
  */
class FilterCategoryUtilsTest {
  var MORETV:String=_
  var MEDUSA:String=_

  @Before def initialize() {
        MORETV = "moretv"
        MEDUSA = "medusa"
  }

  @Test
  def getFilterCategoryFirst: Unit ={
    val testCaseList = List(
      ("home*classification*movie-movie-retrieval*hot*kongbu*qita*all","xxx",MEDUSA,"hot","kongbu","qita","all", "movie"),
      ("home*classification*movie-movie-retrieval*hot*xiju*all*all","xxx",MEDUSA,"hot","xiju","all","all", "movie"),
      ("home*my_tv*movie-movie-retrieval*hot*juqing*gangtai*1990*1999","xxx",MEDUSA,"hot","juqing","gangtai","1990*1999", "movie"),
      ("home*my_tv*tv-tv-retrieval*hot*mohuan*neidi*2017","xxx",MEDUSA,"hot","mohuan","neidi","2017", "tv"),
      ("home*classification*movie-movie-retrieval*hot*dongzuo*meiguo*qita","xxx",MEDUSA,"hot","dongzuo","meiguo","qita" ,"movie"),
      ("xxx","home-movie-multi_search-new-all-all-all",MORETV,"new","all","all","all", "movie"),
      ("xxx","home-comic-multi_search-hot-qita-riben-2014-similar",MORETV,"hot","qita","riben","2014", "comic"),
      ("xxx","home-movie-multi_search-score-zhanzheng-all-2014-peoplealsolike",MORETV,"score","zhanzheng","all","2014", "movie"),
      ("xxx","home-tv-multi_search-hot-all-neidi-1990-1999",MORETV,"hot","all","neidi","1990-1999", "tv")

    )
//      testCaseList.foreach(w => {
//        println(w._1+","+w._2+","+w._3+","+w._4+","+w._5+","+w._6+","+w._7)
//        val firstFilterCategory=FilterCategoryUtils.getFilterCategoryFirst(w._1,w._2,w._3)
//        val secondFilterCategory=FilterCategoryUtils.getFilterCategorySecond(w._1,w._2,w._3)
//        val thirdFilterCategory=FilterCategoryUtils.getFilterCategoryThird(w._1,w._2,w._3)
//        val fourthFilterCategory=FilterCategoryUtils.getFilterCategoryFourth(w._1,w._2,w._3)
//        val content_type = FilterCategoryUtils.getFilterCategoryContentType(w._1,w._2,w._3)
//        assertEquals(w._4,firstFilterCategory)
//        assertEquals(w._5,secondFilterCategory)
//        assertEquals(w._6,thirdFilterCategory)
//        assertEquals(w._7,fourthFilterCategory)
//        assertEquals(w._8,content_type)
//    })
  }

}
