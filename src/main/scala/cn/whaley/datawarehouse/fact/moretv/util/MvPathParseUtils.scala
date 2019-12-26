package cn.whaley.datawarehouse.fact.moretv.util

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}


/**
  * Created by wu.jiulin on 2017/4/25.
  *
  * MV列表页入口分两种情况:
  * 1.包含mv_category,mv_category前面的是二级入口，后面的是三级入口(可能没有三级入口)
  *   如:home*live*eagle-mv*function*site_concert-mv_category*华语
  *      home*classification*mv-mv*horizontal*site_mvyear-mv_category
  *   group(1)为二级入口,group(3)为三级入口
  * 2.包含mv_poster,mv_poster前面就是二级入口,只有site_hotsinger,site_mvtop,site_mvsubject三种情况满足,而且只到二级入口
  *   如:mv*function*site_hotsinger-mv_poster
  *   group(1)为二级入口
  */
object MvPathParseUtils {

  private val MAIN_CATEGORY = "mv"
  private val MV_3X_LIST_CATEGORY_REGEX = (".*\\*(site_.*)(-mv_category|-mv_poster)\\**([\\u4e00-\\u9fa5A-Z0-9-&]*)").r

  /**
    * This function is used to parse the ''pathMain'' field in 3x version of medusa
    * @param path
    * @param out_index
    * @return
    */
  def pathMainParse(path:String,out_index:Int) = {
    var res:String = null
    if(path != null && path != "") {
      if (path.contains("mv_poster") || path.contains("mv_category")){
        /**确定为音乐频道相关的路径*/
        val (main_category,second_category,third_category) = parse3xListCategoryInfo(path)
        if (out_index == 1) res = main_category else if (out_index == 2) res = second_category else if (out_index == 3) res = third_category
      }
    }
    res
  }

  /**
    * @param str
    * @return
    */
  def parse3xListCategoryInfo(str: String): (String, String, String) = {
    val main_category = MAIN_CATEGORY
    var second_category:String = null
    var third_category:String = null

    MV_3X_LIST_CATEGORY_REGEX findFirstMatchIn str match {
      case Some(p) => {
        second_category = p.group(1)
        third_category = p.group(3)
      }
      case None =>
    }

    (main_category,second_category,third_category)
  }

  /**
    * Split some string based on by
    * The $string should not be null, the $string should not be the same as $by and the string should contains $by
    * @param str
    * @param by
    * @return
    */
  def splitStr(str:String,by:String) = {

    /**转义*/
    var splitBy:String = null
    if(by.equals("*")) splitBy = "\\*" else splitBy = by

    if(str != null && !str.equals(by) && str.contains(by)) {
      val arr = str.split(splitBy)
      (arr,arr.length)
    }else{
      (Array[String](),0)
    }
  }
}
