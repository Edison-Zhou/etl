package cn.whaley.datawarehouse.fact.moretv.util

import java.sql.Struct

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}
import cn.whaley.datawarehouse.global.LogConfig
import com.alibaba.fastjson.JSON
import org.apache.avro.TestAnnotation
import org.apache.spark.sql.Row

/**
  * Created by michael on 2017/4/24.
  * updated by wu.jiulin on 2017/4/27.
  * 首页入口维度工具类
  */
object EntranceTypeUtils extends LogConfig {

  /**
    * 对于medusa日志live,recommendation,search,setting没有location_code
    * 对于moretv日志只有live,search有对应的路径信息且只有area_code
    */
  private val MEDUSA_ENTRANCE_REGEX = "home\\*(classification|foundation|my_tv)\\*[0-9-]{0,2}([a-z_]*)".r
  private val MEDUSA_ENTRANCE_REGEX_WITHOUT_LOCATION_CODE = "(open_screen|live|recommendation|search|setting|hotSubject|taste|memberArea)".r
  private val MORETV_ENTRANCE_REGEX = "home-(TVlive|live|search|history|watchhistory|hotrecommend)".r
  private val MEDUSA_ENTRANCE_MY_TV_317_REGEX = "home\\*my_tv\\*1-accountcenter_home\\*([a-zA-Z0-9&\\u4e00-\\u9fa5]+)".r
  private val MEDUSA_ENTRANCE_RECOMMEND_REGEX = "^home\\*(hotSubject|taste|recommendation|memberArea)\\*([0-9]+)$".r

  private def getEntranceCodeByPathETL(path: String, flag: String, code: String): String = {
    var result: String = null
    var launcher_area_code: String = null
    var launcher_location_code: String = null
    if (null != path && null != flag && null != code) {
      flag match {
        case MEDUSA => {
          if (path.contains("home*classification") || path.contains("home*foundation") || path.contains("home*my_tv")) {
            MEDUSA_ENTRANCE_REGEX findFirstMatchIn path match {
              case Some(p) => {
                launcher_area_code = p.group(1)
                launcher_location_code = p.group(2)

                //修复317 路径打点问题，在317上"我的电视"模块的历史与收藏已经合并在一起了：home*my_tv*1-accountcenter_home*收藏追看/home*my_tv*1-accountcenter_home*观看历史
                if (launcher_location_code == "accountcenter_home") {
                  MEDUSA_ENTRANCE_MY_TV_317_REGEX findFirstMatchIn path match {
                    case Some(p) => {
                      p.group(1) match {
                        case "观看历史" => launcher_location_code = "history"
                        case "收藏追看" | "明星关注" | "标签订阅" | "节目预约" | "专题收藏" => launcher_location_code = "collect"
                        case _ =>
                      }
                    }
                    case None =>
                  }

                }
              }
              case None =>
            }
          } else {
            MEDUSA_ENTRANCE_REGEX_WITHOUT_LOCATION_CODE findFirstMatchIn path match {
              case Some(p) => {
                launcher_area_code = p.group(1)
              }
              case None =>
            }
          }
        }
        case MORETV => {
          MORETV_ENTRANCE_REGEX findFirstMatchIn path match {
            case Some(p) => {
              val code = p.group(1)
              if(code.equalsIgnoreCase("TVlive")){
                launcher_area_code="live"
              } else if(code.equalsIgnoreCase("hotrecommend")){
                launcher_area_code="recommendation"
              } else if (code.equalsIgnoreCase("history") || code.equalsIgnoreCase("watchhistory")){
                launcher_area_code="my_tv"
                launcher_location_code="history"
              } else {
                launcher_area_code = code
              }
            }
            case None =>
          }
        }
      }

      code match {
        case "area" => result = launcher_area_code
        case "location" => result = launcher_location_code
      }
    }
    result
  }

  def getEntrancePageCode(path_4x: Seq[Row]): String = {
    var pageCode: String = null
    pageCode = "newHomePage4"
    pageCode
  }

  def getEntranceTableCode(path_4x: Seq[Row]): String = {
    var tableCode: String = null
    if(path_4x(0).getAs[String]("page_type")=="LauncherActivity") {
      tableCode = path_4x(0).getAs[String]("access_area")
    }
    tableCode
  }

  def getEntranceAreaCode(pathMain: String, path: String, flag: String): String = {
    var areaCode: String = null
    val code = "area"
    flag match {
      case MEDUSA => {
        areaCode = getEntranceCodeByPathETL(pathMain, flag, code)
      }
      case MORETV => {
        areaCode = getEntranceCodeByPathETL(path, flag, code)
      }
    }
    areaCode
  }

  def getEntranceElementCode(path_4x: Seq[Row]): String = {
    var elementCode: String = null
    if(path_4x(0).getAs[String]("page_type")=="LauncherActivity") {
      elementCode = path_4x(0).getAs[String]("sub_access_area")
    }
    elementCode
  }

  def getEntranceLocationCode(pathMain: String, path: String, path_json: Seq[Row], flag: String): String = {
    var locationCode: String = null
    val code = "location"
    flag match {
      case MEDUSA => {
        locationCode = getEntranceCodeByPathETL(pathMain, flag, code)
      }
      case MORETV => {
        locationCode = getEntranceCodeByPathETL(path, flag, code)
      }
      case MEDUSA4X => {
        if (path_json(0).getAs[String]("page_type") == "LauncherActivity") {
          val tableCode = path_json(0).getAs[String]("access_area")
          if (tableCode == "editor_my" || tableCode == "editor_classification") {
            locationCode = path_json(0).getAs[String]("link_value")
          }
        }
      }
      case UTVMORE => {
        //优视猫的首页location_code逻辑
        if (path_json(0).getAs[String]("page_type") == "LauncherActivity") {
          val tableCode = path_json(0).getAs[String]("access_area")
          if (tableCode == "ysm_my" || tableCode == "ysm_classification") {
            locationCode = path_json(0).getAs[String]("link_value")
          }
        }
      }
    }

    locationCode
  }

  //for main3x
  def getEntranceLocationCode(pathMain: String, path: String, flag: String): String = {
    getEntranceLocationCode(pathMain,path,null,flag)
  }

  def getRecommendLocationIndex(pathMain: String): String = {
    if (null != pathMain) {
      MEDUSA_ENTRANCE_RECOMMEND_REGEX findFirstMatchIn pathMain match {
        case Some(p) => p.group(2)
        case None => null
      }
    }
    else null
  }

  def getEntranceLocationIndex(path_4x: Seq[Row]): String = {
    var locationIndex: String = null
    if (path_4x(0).getAs[String]("page_type") == "LauncherActivity") {
      locationIndex = path_4x(0).getAs[String]("location_index")
    }
    locationIndex
  }


  /** 通过launcher_area_code和launcher_location_code取得launcher_entrance_sk */
  def getLauncherEntranceSK(): DimensionColumn = {
    new DimensionColumn("dim_medusa_launcher_entrance",
      List(
        DimensionJoinCondition(
          /** launcher_location_code is not null,join with launcher_area_code and launcher_location_code. (classification,foundation,my_tv) */
          Map("launcherAreaCode" -> "launcher_area_code", "launcherLocationCode" -> "launcher_location_code"),
          null, null, null
        ),
        DimensionJoinCondition(
          /** launcher_location_code is null,join with launcher_area_code. (live,recommendation,search,setting,hotSubject,taste) */
          Map("launcherAreaCode" -> "launcher_area_code"),
          null, null, null
        )
      ),
      "launcher_entrance_sk")
  }

}
