package cn.whaley.datawarehouse.fact.util

import cn.whaley.datawarehouse.fact.moretv.util.{ListCategoryUtils, Path4XParserUtils, SubjectUtils}
import cn.whaley.datawarehouse.fact.moretv.util.SubjectUtils.MEDUSA4X
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.junit.Assert.assertEquals
import org.junit.{Before, Test}
import cn.whaley.datawarehouse.fact.constant.UDFConstantDimension._
import cn.whaley.datawarehouse.util.{DateFormatUtils, UdfUtils}
import com.alibaba.fastjson.JSON

/**
  * Created by zhu.bingxin 18/09/20
  */
class Path4XParserUtilsTest {
  var path_4x: Seq[Row] = null

  @Before
  def initialize() {
    //    System.setProperty("javax.xml.parsers.DocumentBuilderFactory",
    //      "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl")
    //    System.setProperty("javax.xml.parsers.SAXParserFactory",
    //      "com.sun.org.apache.xerces.internal.jaxp.SAXParserFactoryImpl")
//    val config = new SparkConf()
//    config.setMaster("local[2]")
//    val spark = SparkSession.builder().config(config).getOrCreate()
//    println(spark+"------")
//    val rdd = spark.sparkContext.textFile("D:\\IEDAWorkspace\\DataWarehouseEtlSpark\\src\\test\\scala\\medusa_path_test.log")
//val rdd = spark.sparkContext.textFile("/WORK/whaley/projects/DataWarehouseEtlSpark/src/test/scala/medusa_4x_path_test.log") //请不要删除该路径
//    path_4x = spark.read.json(rdd)
      //第二行记录是首页-》详情页-》详情页-》详情页
      //第三行记录是首页-》详情页-》详情页
      //第四行记录是首页-》详情页
      //第五行记录是首页-》播放页
      //第六行记录是首页-》奇趣首页-》专题
      //第六行记录是首页-》电影首页-》专题-》详情页-》详情页
      //第七行记录是筛选页
      //第八行记录是网络直播页
      //第九行记录的是直播首页的播放记录
  //    .collect()(15)
 //     .getAs[Seq[Row]](0)
  }

  @Test
  def getInfo: Unit = {


    //    println(Path4XParserUtils.getIndexPath(path_4x, 1, PAGE_TYPE))
    //    println(Path4XParserUtils.getIndexPath(path_4x, 2, PAGE_URL))
    //    println(Path4XParserUtils.getIndexPath(path_4x, 3, SUB_SUBJECT_CODE))
    //    println(Path4XParserUtils.getLastPath(path_4x, PAGE_URL))
    //    println(Path4XParserUtils.getRecommendSourceType(path_4x, ""))
    //    println(Path4XParserUtils.getIndexPath(path_4x, 1, PAGE_TYPE))
    //    println(Path4XParserUtils.getIndexPath(path_4x, 2, PAGE_TYPE))
    //    println(Path4XParserUtils.getFilterInfo(path_4x, "content_type"))
    //    println(Path4XParserUtils.getFilterInfo(path_4x, "tag"))
    //    println(Path4XParserUtils.getFilterInfo(path_4x, "sort"))
    //    println(Path4XParserUtils.getFilterInfo(path_4x, "year"))
    //    println(Path4XParserUtils.getFilterInfo(path_4x, "area"))
    //    println(Path4XParserUtils.getHomePageInfo(path_4x))
    //println(Path4XParserUtils.getPreviousSid(path_4x))
//    println(Path4XParserUtils.getLiveSourceSiteCode(path_4x))
//    println(Path4XParserUtils.getCarouselEntrance(path_4x))
//
//    val url = "linkType=100005&linkValue=hot344&contentType=&channelType=&treeSite=&siteCode=&sid=&title="
//    url.split("&")
//    val ip = "223.74.24.137"
//    println(Path4XParserUtils.getLiveSubjectCode(path_4x))
//    Path4XParserUtils.getLayoutPageInfo(path_4x).foreach(println)
//    println(ListCategoryUtils.getListMainCategory("", "", path_4x, "medusa4x"))
//    println(ListCategoryUtils.getListSecondCategory("", "", path_4x, "medusa4x"))
//    println(ListCategoryUtils.getListThirdCategory("", "", path_4x, "medusa4x"))
//    println(ListCategoryUtils.getListFourthCategory("", "", path_4x, "medusa4x"))

  }

  @Test
  def test_rowSeq2String(): Unit ={
  //  println(UdfUtils.rowSeq2String(path_4x))
  }

  def getFixSql(sql: String, date: String): String = {
    var fixSql = sql
    val date2 = DateFormatUtils.enDateAdd(date, 1)
    if (sql.contains("log_medusa_main4x_play")) {
      if (fixSql.contains("key_day")) {
        fixSql = fixSql.replace("$day_p", date).replace("${day_p}", date)
          .concat(" union all ")
          .concat(fixSql.replace("$day_p", date2).replace("${day_p}", date2))
          .concat(" and key_hour in ('00','01')") //多读第二天的2个小时的数据
      } else throw new RuntimeException("sql must contains key_day column")
    } else {
      fixSql = fixSql.replace("$day_p", date).replace("${day_p}", date)
    }
    fixSql
  }

//
//  @Test
//  def test_getFixSql() = {
//    print(getFixSql("select * from ods_view.log_medusa_main4x_live where key_day = $day_p", "20181002"))
//  }

}