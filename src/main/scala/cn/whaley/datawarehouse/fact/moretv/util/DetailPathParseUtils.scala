package cn.whaley.datawarehouse.fact.moretv.util

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}
import cn.whaley.datawarehouse.global.LogConfig
import org.apache.spark.sql.Row

import scala.util.control.Breaks
import scala.util.control.Breaks._

/**
  * Created by yang.qizhen on 2018/9/29.
  *
  * 解析4x路径详情页信息
  */
object DetailPathParseUtils extends LogConfig {

  def getDetailCode(path_4x:Seq[Row],code:String): String = {
    var Code: String = null
    val outter = new Breaks()
    outter.breakable {
      path_4x.reverse.foreach(data => {
        val page_type = data.getAs[String]("page_type")
        //获取contentType
        var contentType: String = ""
        val loop = new Breaks()
        loop.breakable {
          if ( data.getAs[String]("page_url") != null) {
            data.getAs[String]("page_url").split("&").foreach(url => {
                                val kvs = url.split("=")
                                if(kvs(0) == "contentType" && kvs.length > 1){
                                  contentType = kvs(1)
                                  loop.break()
                                }
            })
          }
        }

        if(Code != null) { outter.break() }
        if (page_type == "DetailHomeActivity" && data.getAs[String]("access_area") != null) {
          if (code == "page_code" && contentType.length > 1) {
            Code = "programPosition"+contentType.substring(0,1).toUpperCase + contentType.substring(1)
          }
          else if (code == "table_code") {
            Code = data.getAs[String]("access_area")
          }
          else if (code == "element_code") {
            Code = data.getAs[String]("sub_access_area")
          }
        }
      })
    }
    Code
  }

}
