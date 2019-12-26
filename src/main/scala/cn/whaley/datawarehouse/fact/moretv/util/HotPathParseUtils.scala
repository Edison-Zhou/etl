package cn.whaley.datawarehouse.fact.moretv.util

import org.apache.avro.TestAnnotation

/**
  * Created by yang.qizhen on 2017/11/07.
  * 资讯路径示例如下：
  * home*classification*hot-hot*分类入口*5-hot*科技前沿*电台
  * home*classification*hot-hot*分类入口*4-hot*监控实拍
  */

object HotPathParseUtils {

  private val HOT_CATEGORY_REGEX = (".*(hot-hot|hot-home)\\*(分类入口)\\*(.*[0-9])-(.*[A-Za-z])\\*(.*[\\u4e00-\\u9fa5])").r

  def pathMainParse(path:String,out_index:Int) = {
    var res:String = null
    if(path != null && path != ""){
      if(path.contains("hot-hot") || path.contains("hot-home")){
        if(out_index == 1){
          res = "hot" //關聯時沒用上
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
    HOT_CATEGORY_REGEX findFirstMatchIn str match {
      case Some(p) =>{
        res1 = "site_hot"
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
    val str = "home*classification*hot-hot*分类入口*1-hot*娱乐动态"
    println(pathMainParse(str,3))
  }


}
