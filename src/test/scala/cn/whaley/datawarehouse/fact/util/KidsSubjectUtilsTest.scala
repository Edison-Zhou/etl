package cn.whaley.datawarehouse.fact.util

import cn.whaley.datawarehouse.fact.moretv.util.SubjectUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.Assert.assertEquals
import org.junit.{Before, Test}

/**
  * Created by wujiulin on 2017/5/9.
  */
class KidsSubjectUtilsTest {
  var MORETV: String = _
  var MEDUSA: String = _
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
  def getSubject: Unit = {
    var path_message: String = null
    val moretvTestCaseList = List(//pathSpecial, path,  flag, subject_code, subject_name
      ("", "home-hotrecommend-1-0-jilu106", MORETV, "jilu106", null),
      ("", "home-hotrecommend-5-0-kids176-similar", MORETV, "kids176", null),
      ("", "home-hotrecommend-7-0-peoplealsolike", MORETV, null, null),
      ("", "home-hotrecommend-10-0-mv020-similar", MORETV, "mv020", null),
      ("", "home-hotrecommend-0-0-hot11-similar", MORETV, "hot11", null),
      ("", "home-hotrecommend-1-0-tv123", MORETV, "tv123", null),
      ("", "home-hotrecommend-10-0-mv020",MORETV, "mv020", null),
      ("", "home-hotrecommend-tv506", MORETV, "tv506", null),

      ("", "home-tv-1_tv_area_xianggang", MORETV, null, null),
      ("", "home-movie-movie_xilie-comic153", MORETV, "comic153", null),
      ("", "home-tv-tv_zhuanti-tv428-similar", MORETV, "tv428", null),
      ("", "home-movie-movie_star-movie870", MORETV, "movie870", null),
      ("", "home-hot-hot_zhuanti-sports140", MORETV, "sports140", null),
      ("", "home-zongyi-zongyi_zhuanti-zongyi194", MORETV, "zongyi194", null),
      ("", "home-jilu-jishi_zhuanti-jilu63", MORETV, "jilu63", null),

      ("", "thirdparty_0-jilu106", MORETV, null, null),
      ("", "thirdparty_0-movie-search-JFS", MORETV, null, null),
      ("", "thirdparty_0-kids_home-kids_seecartoon-kids_star-kids98-similar", MORETV, "kids98", null),
      ("", "thirdparty_0-tv-tv_zhuanti-tv495", MORETV, "tv495", null),
      ("", "thirdparty_0-movie-movie_jujiaodian-movie992", MORETV, "movie992", null),
      ("", "thirdparty_0-hot-hot_zhuanti-hot486", MORETV, "hot486", null),
      ("", "thirdparty_0-comic-comic_zhujue-comic268", MORETV, "comic268", null),
      ("", "thirdparty_0-zongyi-zongyi_zhuanti-zongyi237", MORETV, "zongyi237", null),

      ("", "home-kids_home-kids_recommend-kids219", MORETV, "kids219", null),
      ("", "home-kids_home-kids_seecartoon-kids_star-kids26", MORETV, "kids26", null),
      ("", "home-kids_home-kids_star-kids194", MORETV, "kids194", null),

      ("", "home-history-collect-subject-kids123", MORETV, "kids123", null),
      ("", "home-history-subjectcollect-subject-jilu93", MORETV, "jilu93", null),
      ("", "home-history-collect-subject-tv516", MORETV, "tv516", null),
      ("", "home-history-collect-subject-zongyi215", MORETV, "zongyi215", null),
      ("", "home-history-collect-subject-comic64", MORETV, "comic64", null),
      ("", "home-history-collect-subject-movie119-similar", MORETV, "movie119", null),
      ("", "home-history-collect-subject-hot478", MORETV, "hot478", null)
    )
    val medusaTestCaseList = List(//pathSpecial, path, flag, subject_code, subject_name
      //sublect_code is not null
      ("subject-不平凡之路-jilu28", "",  MEDUSA, "jilu28", "不平凡之路"),
      ("subject-大脑风暴-zongyi68", "",  MEDUSA, "zongyi68", "大脑风暴"),
      ("subject-高考倒计时-hot312", "",  MEDUSA, "hot312", "高考倒计时"),
      ("subject-周润发-movie397", "",  MEDUSA, "movie397", "周润发"),
      ("subject-老戏骨之陈宝国-tv103", "",  MEDUSA, "tv103", "老戏骨之陈宝国"),
      ("subject-快乐开学-kids82", "", MEDUSA, "kids82", "快乐开学"),
      ("subject-暑假大冒险侦探世界-kid19", "",  MEDUSA, "kid19", "暑假大冒险侦探世界"),
      ("subject-那些年我们追过的美番-comic51", "",  MEDUSA, "comic51", "那些年我们追过的美番"),
      ("subject-做个灵活的胖子-sports125", "",  MEDUSA, "sports125", "做个灵活的胖子"),
      ("subject-爆笑集锦 - 歌坛无敌车祸现场-mv022", "",  MEDUSA, "mv022", "爆笑集锦 - 歌坛无敌车祸现场"),
      //subject_code is null
      ("subject-猫和老鼠", "",  MEDUSA, null, "猫和老鼠"),
      ("subject-王者秘籍", "", MEDUSA, null, "王者秘籍"),
      ("subject-召唤童年记忆", "",  MEDUSA, null, "召唤童年记忆")
    )
    val medusa4xTestCaseList = List(
   //   ("","",path_4x,"medusa4x","LauncherActivity&editor_recommend",null)
    )
    val testCaseList = medusa4xTestCaseList

    testCaseList.foreach(f = w => {
//      if (w._1 == "") path_message = w._2 else path_message = w._1
//      val subjectCode:String = SubjectUtils.getSubjectCodeByPathETL(w._1, w._2, path_4x,w._4)
//      val subSubjectCode:String = SubjectUtils.getSubSubjectCodeByPath4xETL(path_4x)
//      val subjectName = SubjectUtils.getSubjectNameByPathETL(w._1)
    //    val subjectEntrance = SubjectUtils.getSubjectEntranceByPath4xETL(w._3)
      try {
//        assertEquals("hot1713",subSubjectCode)
//        assertEquals(w._5, subjectCode)
//        assertEquals(w._6, subjectName)
   //       assertEquals(w._5, subjectEntrance)
      } catch {
        case e: AssertionError =>
          println("fail test case: " + path_message)
          throw e
      }

    })

  }


}
