package cn.whaley.datawarehouse.fact.moretv.util

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}
import org.apache.avro.TestAnnotation

/**
  * Created by yang.qizhen on 2017/4/25.
  * 奇趣路径示例如下：
  * home*my_tv*interest-interest*小窗播放*1
  * home*my_tv*interest-interest*热词区*1
  * home*classification*interest-interest*首屏专题推荐区*3
  * home*classification*interest-interest*分类入口*4-interest*收藏
  * home*classification*interest-interest*分类入口*4-interest-search*QQD
  * home*classification*interest-interest*分类入口*5-interest*游戏动画*电台
  * home*my_tv*interest-interest*分类入口*1-interest*文艺咖
  * home*classification*interest-interest*专题推荐区*1-column*【综合】绝不能错过*4（注：5层）
  * home*my_tv*interest-interest*单片人工推荐区*4
  * home*classification*interest-interest*单片订阅推荐区*17-interest
  */

object InterestPathParseUtils {

  private val INTEREST_CATEGORY_REGEX = (".*(interest-interest|interest-home)\\*(分类入口)\\*(.*[0-9])-(.*[A-Za-z])\\*(.*[\\u4e00-\\u9fa5])").r

  def pathMainParse(path:String,out_index:Int) = {
    var res:String = null
    if(path != null && path != ""){
      if(path.contains("interest-interest") || path.contains("interest-home")){
        if(out_index == 1){
          res = "interest"
        }else{
          val (area,tab) = parse3xListCategoryInfo(path)
          if(out_index == 2) res = area  else if(out_index == 3) res = tab
        }
      }
    }
    res
  }

  def parse3xListCategoryInfo(str:String) = {
    var res1:String = null
    var res2:String = null

    /**Step 1: 站点树*/
    INTEREST_CATEGORY_REGEX findFirstMatchIn str match {
      case Some(p) =>{
        res1 = "site_interest"
        if(p.group(5).contains("*")){
          res2 = p.group(5).split("[*]")(0)
        }
        else{
          res2 = p.group(5)
        }
      }
      case None =>
    }
    (res1,res2)
  }



  @TestAnnotation
  def main(args: Array[String]): Unit = {
    val str = "home*classification*interest-interest*分类入口*4-interest*收藏"
    println(pathMainParse(str,3))
  }


}
