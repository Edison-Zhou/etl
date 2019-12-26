package cn.whaley.datawarehouse.fact.moretv.util

import java.sql.Struct

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}
import cn.whaley.datawarehouse.global.LogConfig
import com.alibaba.fastjson.JSON
import org.apache.spark.sql.Row

import scala.util.control.Breaks._

/**
  * Created by michael on 2017/5/3.
  */
object SearchUtils extends LogConfig {

  private val regex_search_medusa = (".*-([\\w]+)-search\\*([\\w]+)[\\*]?.*").r
  private val regex_search_medusa_home = ("(home)-search\\*([\\w]+)[\\*]?.*").r
  private val regex_search_moretv = (".*-([\\w]+)-search-([\\w]+)[-]?.*").r
  private val regex_search_moretv_home = ("(home)-search-([\\w]+)[-]?.*").r

  def getSearchFrom(pathMain: String, path: String, path_4x: Seq[Row],flag: String): String = {
    var result: String = null
    flag match {
      case MEDUSA => {
        result = getSearchDimension(pathMain, 1, flag)
      }
      case MORETV => {
        result = getSearchDimension(path, 1, flag)
      }
      case MEDUSA4X => {
          var i = -1
          path_4x.foreach(data => {
            i = i + 1
            if (data.getAs[String]("page_type") == "SearchActivity") {

          //    if(channelType == null) throw new RuntimeException(path_4x.toString()+" , "+path_4x(i - 1).toString())
             if(i == 0){
               result = "third_jump"
             }
              else
              {
                val pre_page_type = path_4x(i - 1).getAs[String]("page_type")

                //获取channelType
                var channelType: String = null
                breakable {
                  if (path_4x(i - 1).getAs[String]("page_url") != null) {
                    path_4x(i - 1).getAs[String]("page_url").split("&").foreach(url => {
                      val kvs = url.split("=")
                      if (kvs(0) == "channelType" && kvs.length > 1) {
                        channelType = kvs(1)
                        break()
                      }
                    })
                  }
                }

                //获取treeSite
                var treeSite: String = null
                breakable {
                  if (path_4x(i - 1).getAs[String]("page_url") != null) {
                    path_4x(i - 1).getAs[String]("page_url").split("&").foreach(url => {
                      val kvs = url.split("=")
                      if (kvs(0) == "treeSite" && kvs.length > 1) {
                        treeSite = kvs(1)
                        break()
                      }
                    })
                  }
                }

                pre_page_type match {
                  case "LauncherActivity" => result = "home"
                    // 406 详情页顶栏增加搜索入口
                  case "DetailHomeActivity" => result = "detailHome"
                  case "KidsAnimActivity" => result = "kandonghua"
                  case "KidsRhymesActivity" => result = "tingerge"
                  case "KidsCollectActivity" => result = "xuezhishi"
                  case "VodActivity" if channelType == "kids" => result = treeSite
                  case "VodActivity" => result = channelType
                  case _ => result
                }
              }
            }
          })
      }
    }
    result
  }

  //for main3x
  def getSearchFrom(pathMain: String, path: String,flag: String): String = {
    getSearchFrom(pathMain,path,null,flag)
  }

  def getSearchAreaCode(path_4x: Seq[Row]): String ={
    var area_code: String = null
    path_4x.foreach(data => {
      if (data.getAs[String]("page_type") == "SearchActivity") {
        area_code = data.getAs[String]("access_area")
      }
    })
    area_code
  }

  def getSearchTab(path_4x: Seq[Row]): String ={
    var search_tab: String = null
    path_4x.foreach(data => {
      if (data.getAs[String]("page_type") == "SearchActivity") {
        search_tab = data.getAs[String]("sub_access_area")
      }
    })
    search_tab
  }

  def getSearchContent(path_4x: Array[Struct]): String ={
    var search_content: String = null
    path_4x.foreach(data => {
      if (JSON.parseObject(data.toString).getString("page_type") == "SearchActivity") {
        search_content = JSON.parseObject(data.toString).getString("search_content")
      }
    })
    search_content
  }

  def getSearchKeyword(pathMain: String, path: String, flag: String): String = {
    var result: String = null
    flag match {
      case MEDUSA => {
        result = getSearchDimension(pathMain, 2, flag)
      }
      case MORETV => {
        result = getSearchDimension(path, 2, flag)
      }
    }
    result
  }

  private def getSearchDimension(path: String, index: Int, flag: String): String = {
    var result: String = null
    if (null != path) {
      flag match {
        case MEDUSA => {
          regex_search_medusa findFirstMatchIn path match {
            case Some(p) => {
              result = p.group(index)
            }
            case None =>
          }
          regex_search_medusa_home findFirstMatchIn path match {
            case Some(p) => {
              result = p.group(index)
            }
            case None =>
          }
        }
        case MORETV => {
          regex_search_moretv findFirstMatchIn path match {
            case Some(p) => {
              result = p.group(index)
            }
            case None =>
          }
          regex_search_moretv_home findFirstMatchIn path match {
            case Some(p) => {
              result = p.group(index)
            }
            case None =>
          }
        }
      }
    }
    result
  }

  /** extraPath, mostpeoplelike: 1, null: 0 */
  def isSearchFromHotWord(extraPath: String): Int = {
    var result: Int = 0
    if (extraPath != null) {
      if (extraPath == "mostpeoplelike") {
        result = 1
      }
    }
    result
  }

 /** 搜索维度表，获得sk
  事实表中字段                              维度表中字段
  searchFrom  (udf解析出字段)        对应   search_from
  resultIndex (日志自带)            对应    search_result_index
  tabName     (日志自带)            对应    search_tab
  extraPath   (日志自带)            对应    search_from_hot_word
  */
 def getSearchSK(): DimensionColumn = {
   new DimensionColumn("dim_medusa_search",
     List(
       DimensionJoinCondition(
         Map("searchFrom" -> "search_from","resultIndex" -> "search_result_index","tabName" -> "search_tab","searchFromHotWord" -> "search_from_hot_word"),
         null, null, null
       )
     ),
     "search_sk")
 }
}
