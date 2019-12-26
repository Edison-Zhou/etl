package cn.whaley.datawarehouse.fact.kids

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}
import cn.whaley.datawarehouse.global.LogConfig
import org.apache.spark.sql.Row

import scala.util.control.Breaks.{break, breakable}


/**
  * Created by yang.qizhen on 2019/3/18.
  * 收集所有关于专题的工具类到此类中
  */
object KidsSubjectUtils extends LogConfig {

  /**
    * 从路径中获取专题code
    */

  def getSubjectCodeFromKidsPath(path_4x:Seq[Row]) :String={
    var pageUrl:String = null
    var second_from_end:String = null
    var third_from_end:String = null
    var fourth_from_end:String = null
    val len = path_4x.length - 1
    var one_from_end = path_4x(len).getAs[String]("page_type")
    if(len >= 1){
      second_from_end = path_4x(len - 1).getAs[String]("page_type")
    }
    if(len >= 2) {
      third_from_end = path_4x(len - 2).getAs[String]("page_type")
    }
    if(len >= 3) {
      fourth_from_end = path_4x(len - 3).getAs[String]("page_type")
    }
    //长视频专题(高配)
    if(one_from_end == "PlayActivity" && second_from_end == "DetailHomeActivity" && third_from_end == "SubjectHomeActivity"){
      pageUrl = path_4x(len-2).getAs[String]("page_url")
    }
    else if  (one_from_end == "PlayActivity" && second_from_end == "DetailHomeActivity"  && third_from_end == "DetailHomeActivity" && fourth_from_end == "SubjectHomeActivity"){
      pageUrl = path_4x(len-3).getAs[String]("page_url")
    }
    //长视频专题(低配)
    else if  (one_from_end == "DetailHomeActivity"  && second_from_end == "DetailHomeActivity" && third_from_end == "SubjectHomeActivity"){
      pageUrl = path_4x(len-2).getAs[String]("page_url")
    }
    else if  (one_from_end == "DetailHomeActivity"  && second_from_end == "SubjectHomeActivity"){
      pageUrl = path_4x(len-1).getAs[String]("page_url")
    }
    //短视频组合专题，取子专题
    else if (one_from_end == "GroupSubjectActivity"){
      pageUrl = path_4x(len).getAs[String]("page_url")
    }
    //短视频其他通用专题
    else if (one_from_end == "SubjectHomeActivity"){
      pageUrl = path_4x(len).getAs[String]("page_url")
    }

    //获取linkValue
    var linkValue: String = null
    breakable {
      if (pageUrl != null) {
        pageUrl.split("&").foreach(url => {
          val kvs = url.split("=")
          if (kvs(0) == "linkValue" && kvs.length > 1) {
            linkValue = kvs(1)
            break()
          }
        })
      }
    }
    
    linkValue
  }

}