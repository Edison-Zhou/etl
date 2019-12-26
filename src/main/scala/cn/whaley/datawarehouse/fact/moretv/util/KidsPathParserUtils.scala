package cn.whaley.datawarehouse.fact.moretv.util

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}
import org.apache.avro.TestAnnotation

/**
  * Created by chubby on 2017/3/30.
  */
object KidsPathParserUtils {

  private val KIDS_3X_RECOMMEND_REGEX = ("(.*)(kids_home|kids)$").r
  // 3x少儿频道主页推荐位正则
  private val KIDS_3X_LIST_CATEGORY_REGEX = ("([a-zA-Z0-9_]+)\\*([\\u4e00-\\u9fa5A-Z0-9-]+)").r
  // 3x少儿频道站点树正则
  private val KIDS_3X_LIST_CATEGORY_SEARCH_REGEX = ("(.*)-(search)\\*([A-Z]+)$").r // 3x少儿频道站点树中的搜索正则

  private val KIDS_2X_RECOMMEND_REGEX = ("(.*)(kids_recommend)-(0-9a-zA-Z)$").r
  // 2x少儿频道主页推荐位正则
  private val KIDS_2X_LIST_CATEGORY_REGEX = ("(.*)-(kids_home)-([0-9a-z_]+)-(.*)").r
  // 2x少儿频道站点树正则
  private val KIDS_2X_LIST_CATEGORY_SEARCH_REGEX = ("(.*)-(search)-([A-Z]+)$").r // 2x少儿频道站点树中的搜索正则


  /**
    * This function is used to parse the ''pathMain'' field in 3x version of medusa
    *
    * @param path
    * @param out_index
    * @return
    */
  def pathMainParse(path: String, out_index: Int) = {
    var res: String = null
    if (path != null && path != "") {
      if (path.contains("kids") || path.contains("tingerge") || path.contains("kandonghua") || path.contains("xuezhishi")) {
        /** 确定为少儿频道相关的路径 */
        if (out_index == 1) {
          res = "kids"
        } else {
          val (category, tab) = parse3xListCategoryInfo(path)
          if (out_index == 2) res = category else if (out_index == 3) res = tab
        }
      }
    }
    res
  }


  /**
    * This function is used to parse the ''path'' field in 2x version of moretv
    *
    * @param path
    * @param out_index
    */
  def pathParse(path: String, out_index: Int) = {
    var res: String = null
    if (path != null && path != "") {
      if (path.contains("kids")) {
        if (out_index == 1) {
          res = "kids"
        } else {
          val (category, tab) = parse2xListCategoryInfo(path)
          if (out_index == 2) res = category else if (out_index == 3) res = tab
        }
      }
    }
    res
  }


  /**
    * @param str
    * @return
    */
  def parse3xListCategoryInfo(str: String) = {
    var res1: String = null
    var res2: String = null
    /** Step 1: 判断是否是推荐信息 */
    val (pathArr, len) = splitStr(str, "-")
    if (len >= 1) {
      KIDS_3X_RECOMMEND_REGEX findFirstMatchIn pathArr(len - 1) match {
        case Some(p) => {
          res1 = "kids_recommend"
        }
        case None =>
      }
    }


    /** Step 2: 判断是否是站点树 */
    KIDS_3X_LIST_CATEGORY_REGEX findFirstMatchIn str match {
      case Some(p) => {
        res1 = p.group(1)
        res2 = p.group(2)
      }
      case None =>
    }

    /** Step 3: 判断是否是搜索的站点树 */
    KIDS_3X_LIST_CATEGORY_SEARCH_REGEX findFirstMatchIn str match {
      case Some(p) => {
        val (p1Arr, len) = splitStr(p.group(1), "-")
        if (len >= 1) {
          res1 = p1Arr(len - 1)
          res2 = "搜一搜"
        }
      }
      case None =>
    }

    (res1, res2)
  }

  /**
    *
    * @param str
    * @return
    */
  def parse2xListCategoryInfo(str: String) = {
    var res1: String = null
    var res2: String = null

    /** Step 1: 判断是否是推荐信息 */
    KIDS_2X_RECOMMEND_REGEX findFirstMatchIn str match {
      case Some(p) => res1 = p.group(2)
      case None =>
    }


    /** Step 2: 判断是否是站点树 */
    KIDS_2X_LIST_CATEGORY_REGEX findFirstMatchIn str match {
      case Some(p) => {
        res1 = p.group(3)
        val (p4Arr, len) = splitStr(p.group(4), "-")
        if (len >= 1) {
          res2 = p4Arr(0)
        }
      }
      case None =>
    }

    /** Step 3: 判断是否是搜索的站点树 */
    KIDS_2X_LIST_CATEGORY_SEARCH_REGEX findFirstMatchIn str match {
      case Some(p) => {
        val (p1Arr, len) = splitStr(p.group(1), "-")
        if (len >= 1) {
          res1 = p1Arr(len - 1)
          res2 = "搜一搜"
        }
      }
      case None =>
    }
    (res1, res2)
  }

  /**
    * Split some string based on by
    * The $string should not be null, the $string should not be the same as $by and the string should contains $by
    *
    * @param str
    * @param by
    * @return
    */
  def splitStr(str: String, by: String) = {

    /** 转义 */
    var splitBy: String = null
    if (by.equals("*")) splitBy = "\\*" else splitBy = by

    if (str != null && !str.equals(by) && str.contains(by)) {
      val arr = str.split(splitBy)
      (arr, arr.length)
    } else {
      (Array[String](), 0)
    }
  }


  @TestAnnotation
  def main(args: Array[String]): Unit = {
    val str = "home*my_tv*kids-kids_home-tingerge*随便听听"
    KIDS_3X_LIST_CATEGORY_REGEX findFirstMatchIn str match {
      case Some(p) => {
        (1 until p.groupCount + 1).foreach(i => {
          println(p.group(i))
        })
      }
      case None =>
    }
  }

  def kidsListCategoryDimension(): DimensionColumn = {
    new DimensionColumn("dim_web_location",
      List(
        DimensionJoinCondition(
           Map("ipKey" -> "web_location_key")
        ),
        DimensionJoinCondition(
          Map("subjectName" -> "subject_name"),
          null, null, null
        )
      ), "web_location_sk")
  }
}
