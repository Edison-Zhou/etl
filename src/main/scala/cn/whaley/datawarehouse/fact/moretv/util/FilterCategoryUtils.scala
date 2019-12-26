package cn.whaley.datawarehouse.fact.moretv.util

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}
import cn.whaley.datawarehouse.global.LogConfig
/**
  * Created by michael on 2017/5/2.
  * Updated by wujiulin on 2017/5/11.
  *  1.解析出content_type
  *  2.获得retrieval_sk
  */
object FilterCategoryUtils extends LogConfig{
  private val regex_moretv_filter = (".*(movie|tv|zongyi|jilu|comic|xiqu|hot|mv|kids|sports)-multi_search-(hot|new|score)-([\\w]+)-([\\w]+)-([\\w]+)[-]?.*").r
  private val regex_moretv_filter_number = (".*(movie|tv|zongyi|jilu|comic|xiqu|hot|mv|kids|sports)-multi_search-(hot|new|score)-([\\w]+)-([\\w]+)-([\\d]+-[\\d]+)[-]?.*").r
  private val regex_medusa_filter = (".*(movie|tv|zongyi|jilu|comic|xiqu|hot|mv|kids|sports)-retrieval\\*(hot|new|score)\\*([\\w]+)\\*([\\w]+)\\*(all|qita|[\\d]+[\\*\\d]*)").r

  //获取筛选维度【排序方式：最新、最热、得分；标签；地区；年代】
  def getFilterCategory(path: String, index: Int, flag: String): String = {
    var result: String = null
    if (null == path) {
      result = null
    } else if (index > 5) {
      result = null
    } else {
      flag match {
        case MEDUSA => {
          regex_medusa_filter findFirstMatchIn path match {
            case Some(p) => {
              result = p.group(index)
            }
            case None =>
          }
        }
        case MORETV => {
          regex_moretv_filter findFirstMatchIn path match {
            case Some(p) => {
              result = p.group(index)
            }
            case None =>
          }
          regex_moretv_filter_number findFirstMatchIn path match {
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

  def getFilterCategoryContentType(pathMain: String,path:String,flag:String): String = {
    var result: String = null
    flag match {
      case MEDUSA => {
        result = getFilterCategory(pathMain,1,MEDUSA)
      }
      case MORETV => {
        result = getFilterCategory(path,1,MORETV)
      }
    }
    result
  }

  def getFilterCategoryFirst(pathMain: String, path: String, flag: String): String = {
    var result: String = null
    flag match {
      case MEDUSA => {
        result = getFilterCategory(pathMain,2,MEDUSA)
      }
      case MORETV => {
        result = getFilterCategory(path,2,MORETV)
      }
    }
    result
  }

  def getFilterCategorySecond(pathMain: String, path: String, flag: String): String = {
    var result: String = null
    flag match {
      case MEDUSA => {
        result = getFilterCategory(pathMain,3,MEDUSA)
      }
      case MORETV => {
        result = getFilterCategory(path,3,MORETV)
      }
    }
    result
  }

  def getFilterCategoryThird(pathMain: String, path: String, flag: String): String = {
    var result: String = null
    flag match {
      case MEDUSA => {
        result = getFilterCategory(pathMain,4,MEDUSA)
      }
      case MORETV => {
        result = getFilterCategory(path,4,MORETV)
      }
    }
    result
  }

  def getFilterCategoryFourth(pathMain: String, path: String, flag: String): String = {
    var result: String = null
    flag match {
      case MEDUSA => {
        result = getFilterCategory(pathMain,5,MEDUSA)
      }
      case MORETV => {
        result = getFilterCategory(path,5,MORETV)
      }
    }
    result
  }

  /** 筛选维度表，获得retrieval_sk
    * 事实表中字段                                      维度表中字段
    * filterCategoryFirst  (udf解析出字段)     对应      sort_type
    * filterCategorySecond (udf解析出字段)     对应      filter_category_first
    * filterCategoryThird  (udf解析出字段)     对应      filter_category_second
    * filterCategoryFourth (udf解析出字段)     对应      filter_category_third
    * filterContentType    (udf解析出字段)     对应      content_type
    */
  def getRetrievalSK() :DimensionColumn = {
    new DimensionColumn("dim_medusa_retrieval",
      List(
        DimensionJoinCondition(
          Map("filterContentType" -> "content_type","filterCategoryFirst" -> "sort_type",
            "filterCategorySecond" -> "filter_category_first","filterCategoryThird" -> "filter_category_second",
          "filterCategoryFourth" -> "filter_category_third"),
          null, null, null
        )
      ),
      "retrieval_sk")
  }

}
