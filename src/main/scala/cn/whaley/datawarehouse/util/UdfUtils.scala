package cn.whaley.datawarehouse.util

import cn.whaley.datawarehouse.fact.constant.UDFConstantDimension.LOCATION_INDEX
import cn.whaley.datawarehouse.fact.kids.KidsSubjectUtils
import cn.whaley.datawarehouse.fact.moretv.FactMerge.{OwnMemberEntranceUtils, TencentMemberEntranceUtils}
import cn.whaley.datawarehouse.fact.moretv.util.Path4XParserUtils._
import cn.whaley.datawarehouse.fact.moretv.util.PathParserUtils
import cn.whaley.datawarehouse.fact.moretv.util._
import cn.whaley.datawarehouse.fact.moretv.util.SubjectUtils
import com.alibaba.fastjson.{JSONArray, JSONObject}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by zhu.bingxin on 2018/9/12.
  */
object UdfUtils {

  def register(spark: SparkSession): Unit = {

    spark.udf.register("subjectPathParse", SubjectUtils.getSubjectCodeByPathETL: (String, String, Seq[Row], String) => String)
    spark.udf.register("get_location_index", (path: Seq[Row]) => getLastPath(path, LOCATION_INDEX))
    spark.udf.register("get_recommend_log_type", (path: Seq[Row], alg: String) => getRecommendSourceType(path, alg))
    spark.udf.register("get_previous_sid", (path: Seq[Row]) => getPreviousSid(path))

    /**
      * 获取筛选的排序方式、地区、年代、类型、频道类型
      */
    spark.udf.register("get_filter_sort", (path: Seq[Row]) => getFilterInfo(path, "sort"))
    spark.udf.register("get_filter_area", (path: Seq[Row]) => getFilterInfo(path, "area"))
    spark.udf.register("get_filter_year", (path: Seq[Row]) => getFilterInfo(path, "year"))
    spark.udf.register("get_filter_tag", (path: Seq[Row]) => getFilterInfo(path, "tag"))
    spark.udf.register("get_filter_content_type", (path: Seq[Row]) => getFilterInfo(path, "content_type"))

    /**
      * 获取大首页和频道首页的pageCode、accessArea、subAccessArea、locationIndex
      */
    spark.udf.register("get_own_page_code", (path: Seq[Row]) => getLayoutPageInfo(path)(0))
    spark.udf.register("get_own_access_area", (path: Seq[Row]) => getLayoutPageInfo(path)(1))
    spark.udf.register("get_own_sub_access_area", (path: Seq[Row]) => getLayoutPageInfo(path)(2))
    spark.udf.register("get_own_location_index", (path: Seq[Row]) => getLayoutPageInfo(path)(3))

    spark.udf.register("subjectCodePathParse",SubjectUtils.getSubjectCodeByPathETL: (String, String, Seq[Row], String) => String)
    spark.udf.register("ipKeyPathParse",getIpKey: String => Long)
    spark.udf.register("mainCategoryPathParse",ListCategoryUtils.getListMainCategory: (String, String, Seq[Row], String) => String)
    spark.udf.register("secondCategoryPathParse",ListCategoryUtils.getListSecondCategory: (String, String, Seq[Row], String) => String)
    spark.udf.register("thirdCategoryPathParse",ListCategoryUtils.getListThirdCategory: (String, String, Seq[Row], String) => String)
    spark.udf.register("fourthCategoryPathParse",ListCategoryUtils.getListFourthCategory: (String, String, Seq[Row], String) => String)
    spark.udf.register("launcherPageCodePathParse", EntranceTypeUtils.getEntrancePageCode: Seq[Row] => String)
    spark.udf.register("launcherTableCodePathParse", EntranceTypeUtils.getEntranceTableCode: Seq[Row] => String)
    spark.udf.register("launcherElementCodePathParse", EntranceTypeUtils.getEntranceElementCode: Seq[Row] => String)
    spark.udf.register("launcherLocationCodePathParse", EntranceTypeUtils.getEntranceLocationCode: (String, String, Seq[Row],String) => String)
    spark.udf.register("launcherLocationIndexPathParse", EntranceTypeUtils.getEntranceLocationIndex: Seq[Row] => String)
    spark.udf.register("searchFromPathParse", SearchUtils.getSearchFrom: (String, String, Seq[Row],String) => String)
    spark.udf.register("searchAreaCodePathParse", SearchUtils.getSearchAreaCode: Seq[Row] => String)
    spark.udf.register("searchTabPathParse", SearchUtils.getSearchTab: Seq[Row] => String)
  //  spark.udf.register("SearchContentPathParse", SearchUtils.getSearchContent: (Seq[Row]) => String)
    spark.udf.register("detailPageCodePathParse", DetailPathParseUtils.getDetailCode: (Seq[Row],String) => String)
    spark.udf.register("detailTableCodePathParse", DetailPathParseUtils.getDetailCode: (Seq[Row],String) => String)
    spark.udf.register("detailElementCodePathParse", DetailPathParseUtils.getDetailCode: (Seq[Row],String) => String)
    spark.udf.register("dimDateParse", getDimDate: String => String)
    spark.udf.register("dimTimeParse", getDimTime: String => String)
    spark.udf.register("rowSeq2String", rowSeq2String: Seq[Row] => String)

    spark.udf.register("subjectEntrancePathParse",SubjectUtils.getSubjectEntranceByPath4xETL: Seq[Row] => String)
    spark.udf.register("channelEntrancePathParse",Path4XParserUtils.getChannelPlayEntrance: Seq[Row] => String)
    spark.udf.register("memberEntrance3x",TencentMemberEntranceUtils.getMemberEntrance3x: (String,String,String) => String)
    spark.udf.register("memberEntrance4x",TencentMemberEntranceUtils.getMemberEntrance4x: (String,Seq[Row],String) => String)
    spark.udf.register("ownMemberEntrance4x",OwnMemberEntranceUtils.getMemberEntrance4x: (Seq[Row],java.lang.Long,java.lang.Long) => String)

    /**
      * 获取直播页上一级的tab_code
      */
    spark.udf.register("getLiveSourceSiteCode", getLiveSourceSiteCode: Seq[Row] => String)
    spark.udf.register("getLiveLauncherEntrance4x", PathParserUtils.getLiveLauncherEntrance4X _)
    spark.udf.register("getLiveLauncherEntrance3x", PathParserUtils.getLiveLauncherEntrance3X _)
    spark.udf.register("get_live_main_subject_code", (path: Seq[Row]) => getLiveSubjectCode(path)(0))
    spark.udf.register("get_live_subject_code", (path: Seq[Row]) => getLiveSubjectCode(path)(1))
    //add by wangning-20190409
    spark.udf.register("get_carousel_entrance",Path4XParserUtils.getCarouselEntrance :Seq[Row] => String)

    /**
      * 根据path获取播放来源页
      */
    spark.udf.register("getPlaySourcePage", getPlaySourcePage: Seq[Row] => String)

    spark.udf.register("kidsSubjectCode", KidsSubjectUtils.getSubjectCodeFromKidsPath: Seq[Row] => String)


    /**
      * 获取优视猫大首页和频道首页的pageCode、accessArea、subAccessArea、locationIndex
      */
    spark.udf.register("get_utv_launcher_page_code", PathUtvmoreParserUtils.getEntrancePageCode: Seq[Row] => String)
    spark.udf.register("get_utv_own_page_code", (path: Seq[Row]) => PathUtvmoreParserUtils.getLayoutPageInfo(path)(0))
    spark.udf.register("get_utv_own_access_area", (path: Seq[Row]) => PathUtvmoreParserUtils.getLayoutPageInfo(path)(1))
    spark.udf.register("get_utv_own_sub_access_area", (path: Seq[Row]) => PathUtvmoreParserUtils.getLayoutPageInfo(path)(2))
    spark.udf.register("get_utv_own_location_index", (path: Seq[Row]) => PathUtvmoreParserUtils.getLayoutPageInfo(path)(3))
  }


  def getIpKey(ip: String): Long = {
    try {
      val ipInfo = ip.split("\\.")
      if (ipInfo.length >= 3) {
        (((ipInfo(0).toLong * 256) + ipInfo(1).toLong) * 256 + ipInfo(2).toLong) * 256
      } else 0
    } catch {
      case ex: Exception => 0
    }
  }

  def getDimDate(dateTime: String): String = {
    try {
      val dateTimeInfo = dateTime.split(" ")
      if (dateTimeInfo.length >= 2) {
        dateTimeInfo(0)
      } else ""
    } catch {
      case ex: Exception => ""
    }
  }

  def getDimTime(dateTime: String): String = {
    try {
      val dateTimeInfo = dateTime.split(" ")
      if (dateTimeInfo.length >= 2) {
        dateTimeInfo(1)
      } else ""
    } catch {
      case ex: Exception => ""
    }
  }

  def rowSeq2String(sr: Seq[Row]):String = {
    if(sr != null){
      val jsonArray = new JSONArray()
      sr.foreach(row => {
        val json = new JSONObject()
        val schema = row.schema.fields
        (0 until row.length).foreach(i => {
          val value = row.getString(i)
          val structField = schema(i)
          val key = structField.name
          json.put(key,value)
        })
        jsonArray.add(json)
      })
      jsonArray.toString
    }else null
  }

}