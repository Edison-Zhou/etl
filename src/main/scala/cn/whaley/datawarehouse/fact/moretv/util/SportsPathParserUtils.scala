package cn.whaley.datawarehouse.fact.moretv.util

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}
import org.apache.avro.TestAnnotation

/**
  * Created by chubby on 2017/3/30.
  */
object SportsPathParserUtils {

  private val SPORTS_HOME_REGEX = ("(.*)sports\\*(recommend|newsHomePage|welfareHomePage|horizontal)(.*)").r
  private val SPORTS_HOME_LIST_CATEGORY_REGEX = ("(.*)sports\\*(collect|League)\\*([0-9a-zA-Z_]+)(.*)").r
  private val SPORTS_LIST_CATEGORY_REGEX = ("([a-zA-Z0-9_]+)\\*([\\u4e00-\\u9fa5A-Z0-9a-z]+)").r



  def pathMainParse(path:String,out_index:Int) = {
    var res:String = null
    if(path != null && path != ""){
      if(path.contains("sports*")){
        if(out_index == 1) {
          res = "sports"
        }else{
          val (area,category,tab) = parse3xListCategoryInfo(path)
          if(out_index == 2) res = area else if(out_index == 3) res = category else if(out_index == 4) res = tab
        }
      }
    }
    res
  }


  /**
    *
    * @param str
    */
  def parse3xListCategoryInfo(str:String) = {
    var res1:String = null
    var res2:String = null
    var res3:String = null

    /**Step 1: 判断不存在站点树*/
    SPORTS_HOME_REGEX findFirstMatchIn str match {
      case Some(p) =>{
        res1 = "horizontal"
        res2 = p.group(2)
        if(res2 == "horizontal") {
          val pg3 =p.group(3)
          if(pg3.contains("collect")){
            res1 = "collect"
            val (pg3Arr,len) = splitStr(pg3,"-")
            if(len>0) {
              SPORTS_LIST_CATEGORY_REGEX findFirstMatchIn pg3Arr(len - 1) match {
                case Some(p) => {
                  res2 = p.group(1)
                  res3 = p.group(2)
                }
                case None =>
              }
            }
          }
        }
      }
      case None =>
    }

    /**Step 2: 判断站点树*/
    SPORTS_HOME_LIST_CATEGORY_REGEX findFirstMatchIn str match {
      case Some(p) => {
        res1 = p.group(2)
        res2 = p.group(3)
        val pg4 = p.group(4)
        val (pg3Arr,len) = splitStr(pg4,"-")
        if(len>0){
        SPORTS_LIST_CATEGORY_REGEX findFirstMatchIn pg3Arr(len-1) match {
          case Some(p) => res3 = p.group(2)
          case None =>
        }
       }
      }
      case None =>
    }

    (res1,res2,res3)

  }


  /**
    * Split some string based on by
    * The $string should not be null, the $string should not be the same as $by and the string should contains $by
    *
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

  @TestAnnotation
  def main(args: Array[String]): Unit = {
    val str = "sports*League*CBA-league"
    println(pathMainParse(str,3))
  }


  def sportListCategoryDimension() :DimensionColumn = {
    new DimensionColumn("dim_web_location",
      List(DimensionJoinCondition(Map("ipKey" -> "web_location_key"))), "web_location_sk")
  }

}
