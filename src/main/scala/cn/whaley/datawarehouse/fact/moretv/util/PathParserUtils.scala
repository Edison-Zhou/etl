package cn.whaley.datawarehouse.fact.moretv.util

import cn.whaley.datawarehouse.fact.constant.UDFConstantDimension
import cn.whaley.datawarehouse.fact.constant.UDFConstantDimension._
import org.apache.spark.sql.Row

/**
  * Created by michael.wang on 2017/4/24.
  */
object PathParserUtils {

  private val HISTORY_COLLECT_LIST = List("launHistory", "launRecentWatched") //4X首页历史和收藏的link_value值

  /**
    * 该对象用于解析路径信息
    * 输入参数说明：
    * logType:指定日志的类型
    * path:指定日志中需要解析的内容
    * path:指定日志中path的字段信息
    * outType:指定解析出的内容
    * 输出说明：
    * 根据outType产生相应的输出结果
    */

  def pathParser(logType: String, path: String, pathType: String, outputType: String) = {
    // 要求logType不为空
    require(logType != null)

    var result: String = null
    // 根据不同的logType进行解析
    logType match {

      /**
        * logType: interview
        */
      case UDFConstantDimension.INTERVIEW => {
        pathType match {
          // moretv的interview的日志只有path这一个路径信息
          case UDFConstantDimension.PATH => {
            outputType match {
              // 获取moretv的contentType信息
              case UDFConstantDimension.CONTENTTYPE => {
                if (path != null) {
                  if (path.contains("-")) {
                    if (path.split("-").length >= 2) {
                      result = path.split("-")(1)
                    }
                  }
                }
              }
              case _ => {
                println(logType + "-" + pathType + "-" + "处理其他outputType!")
              }
            }
          }
          case _ => {
            println(logType + "-" + pathType + "-" + "处理其他pathType!")
          }
        }
      }

      /**
        * logType: detail/play/playview
        */
      case UDFConstantDimension.DETAIL | UDFConstantDimension.PLAY | UDFConstantDimension.PLAYVIEW => {
        pathType match {
          // medusa的detail日志中的pathMain字段
          case UDFConstantDimension.PATHMAIN => {
            outputType match {
              // medusa的pathMain路径中的launcherArea信息
              case UDFConstantDimension.LAUNCHERAREA => {
                result = getPathMainInfo(path, 1, 2)
                if (result != null) {
                  if (!UDFConstantDimension.MedusaLauncherArea.contains(result)) {
                    result = null
                  }
                } else {
                  if (getPathMainInfo(path, 2, 1) == "search" || getPathMainInfo(path, 2, 1) == "setting") {
                    result = "navi"
                  }
                }
              }
              // medusa的pathMain路径中的launcherAccessLocation信息
              case UDFConstantDimension.LAUNCHERACCESSLOCATION => {
                result = getPathMainInfo(path, 1, 3)
                if (result != null) {
                  if (getPathMainInfo(path, 1, 2) == UDFConstantDimension.MedusaLive || !UDFConstantDimension.MedusaLauncherAccessLocation
                    .contains(result)) {
                    result = null
                  }
                } else {
                  // 处理launcher的搜索和设置的点击事件
                  if (getPathMainInfo(path, 2, 1) == "search") {
                    result = "search"
                  } else if (getPathMainInfo(path, 2, 1) == "setting") {
                    result = "setting"
                  }
                }
              }
              // medusa的pathMain路径中的pageType信息
              case UDFConstantDimension.PAGETYPE => {
                result = getPathMainInfo(path, 2, 1)
                if (result != null) {
                  if (!UDFConstantDimension.MedusaPageInfo.contains(result)) {
                    result = null
                  }
                }
              }
              // medusa的pathMain路径中的pageDetailInfo信息
              case UDFConstantDimension.PAGEDETAILINFO => {
                result = getPathMainInfo(path, 2, 2)
                if (result != null) {
                  if (!UDFConstantDimension.MedusaPageDetailInfo.contains(result)) {
                    result = null
                  }
                } else {
                  // 处理少儿频道
                  if (getPathMainInfo(path, 2, 1) == "kids_home") {
                    result = getSplitInfo(path, 3)
                    if (result != null && !UDFConstantDimension.MedusaPageDetailInfo.contains(result)) {
                      result = null
                    }
                    if (getPathMainInfo(path, 3, 2) == null) {
                      if (getPathMainInfo(path, 4, 1) == "search") {
                        result = "搜一搜"
                      }
                    } else if (getPathMainInfo(path, 3, 2) == "搜一搜") {
                      result = "搜一搜"
                    }
                  }

                  // 处理搜索和筛选
                  if (getPathMainInfo(path, 3, 1) == "search" || getPathMainInfo(path, 3, 1) == "retrieval") {
                    result = getPathMainInfo(path, 3, 1)
                  }
                }
              }
            }
          }
          // medusa的detail/play日志中的pathSub字段
          case UDFConstantDimension.PATHSUB => {
            outputType match {
              // medusa的pathSub字段中的访问路径信息
              case UDFConstantDimension.ACCESSPATH => {
                result = getPathMainInfo(path, 1, 1)
                if (!UDFConstantDimension.MedusaPathSubAccessPath.contains(result)) {
                  result = null
                }
              }
              // medusa的pathSub字段中的前一个节目的sid
              case UDFConstantDimension.PREVIOUSSID => {
                if (UDFConstantDimension.MedusaPathSubAccessPath.contains(getPathMainInfo(path, 1, 1))) {
                  result = getPathMainInfo(path, 3, 1)
                }
              }
              // medusa的pathSub字段中的前一个节目的contentType
              case UDFConstantDimension.PREVIOUSCONTENTTYPE => {
                if (UDFConstantDimension.MedusaPathSubAccessPath.contains(getPathMainInfo(path, 1, 1))) {
                  result = getPathMainInfo(path, 3, 2)
                }
              }
            }
          }
          // medusa的detail/play日志中的pathSpecial字段
          case UDFConstantDimension.PATHSPECIAL => {
            outputType match {
              // 获取medusa的pathSpecial中的“路径性质”信息
              case UDFConstantDimension.PATHPROPERTY => {
                result = getPathMainInfo(path, 1, 1)
                if (!UDFConstantDimension.MedusaPathProperty.contains(result)) {
                  result = null
                }
              }
              // 获取medusa的pathSpecial中的“路径标识”信息
              case UDFConstantDimension.PATHIDENTIFICATION => {
                if (UDFConstantDimension.MedusaPathProperty.contains(getPathMainInfo(path, 1, 1))) {
                  val pathLen = path.split("-").length
                  if (pathLen == 2) {
                    result = getPathMainInfo(path, 2, 1)
                  } else if (pathLen > 2) {
                    var tempResult = getPathMainInfo(path, 2, 1)
                    var splitData = path.split("-")
                    for (i <- 2 until pathLen) {
                      tempResult = tempResult.concat("-").concat(getPathMainInfo(path, i + 1, 1))
                    }
                    result = tempResult
                  }
                }
              }
            }
          }
          // moretv的detail日志中的path字段
          case UDFConstantDimension.PATH => {
            if (path.contains("-")) {
              outputType match {
                // moretv的launcherArea信息
                case UDFConstantDimension.LAUNCHERAREA => {
                  result = getSplitInfo(path, 2)
                  if (result != null) {
                    // 如果是属于“热门推荐”、“大家在看”，“卫视直播”，“观看历史”，则area信息就为其本身
                    if (!UDFConstantDimension.MoretvLauncherUPPART.contains(result)) {
                      if (UDFConstantDimension.MoretvLauncherAreaNAVI.contains(result)) {
                        result = "navi" // 包含了"搜索"和“设置”
                      } else if (UDFConstantDimension.MoretvLauncherCLASSIFICATION.contains(result)) {
                        result = "classification" // 包含了launcher页面的下面部分，除了"搜索"和“设置”之外
                      }
                    }
                  }
                }
                // moretv的accessLocation信息
                case UDFConstantDimension.LAUNCHERACCESSLOCATION => {
                  result = getSplitInfo(path, 2)
                  if (result != null) {
                    // 如果accessArea为“navi”和“classification”，则保持不变，即在launcherAccessLocation中
                    if (!UDFConstantDimension.MoretvLauncherAccessLocation.contains(result)) {
                      // 如果不在launcherAccessLocation中，则判断accessArea是否在uppart中
                      if (UDFConstantDimension.MoretvLauncherUPPART.contains(result)) {
                        result match {
                          case "watchhistory" => result = null
                          case "otherwatch" => result = getSplitInfo(path, 3)
                          case "hotrecommend" => result = getSplitInfo(path, 3)
                          case "TVlive" => result = null
                          case _ => result = null
                        }
                      } else {
                        result = null
                      }
                    }
                  }
                }
                // moretv的pageType信息
                case UDFConstantDimension.PAGETYPE => {
                  if (UDFConstantDimension.MoretvPageInfo.contains(getSplitInfo(path, 2))) {
                    result = getSplitInfo(path, 2)
                  }
                }
                // moretv的pageDetailInfo
                case UDFConstantDimension.PAGEDETAILINFO => {
                  result = getSplitInfo(path, 3)
                  if (result != null) {
                    if (getSplitInfo(path, 2) == "search") {
                      result = null
                    }
                    if (getSplitInfo(path, 2) == "kids_home" || getSplitInfo(path, 2) == "sports") {
                      result = getSplitInfo(path, 3) + "-" + getSplitInfo(path, 4)
                    }
                  }
                  // 将English转为Chinese
                  if (UDFConstantDimension.MoretvPageInfo.contains(getSplitInfo(path, 2))) {
                    val page = getSplitInfo(path, 2)
                    if (UDFConstantDimension.MoretvPageDetailInfo.contains(result)) {
                      //result = transformEng2Chinese(page, result)
                    }
                  }
                }

                // 处理moretv的“访问路径”信息
                case UDFConstantDimension.ACCESSPATH => {
                  var len = 0
                  if (path.contains("-")) {
                    len = path.split("-").length
                  } else {
                    len = 1
                  }
                  result = getSplitInfo(path, len)
                  if (!UDFConstantDimension.MORETVPATHSUBCATEGORY.contains(result)) {
                    result = null
                  }
                }
                //目前moretv的path字段中没有上一个节目的sid和contentType的信息
                case UDFConstantDimension.PREVIOUSSID => {
                  result = null
                }
                case UDFConstantDimension.PREVIOUSCONTENTTYPE => {
                  result = null
                }
                case UDFConstantDimension.PATHPROPERTY => {
                  if (path.contains("-tag-")) {
                    result = "tag"
                  } else if (path.contains("-subject-")) {
                    result = "subject"
                  } else if (path.contains("-actor-")) {
                    result = "star"
                  }
                }
                case UDFConstantDimension.PATHIDENTIFICATION => {
                  // 在moretv中没有给pathproperty添加具体的信息，即没有给出actor所对应的具体内容，因此设为null；
                  // tag/subject/的信息可以给出
                  //                  if(path.contains("-subject-")){
                  //                    // 让subjectIndex从1开始表示
                  //                    val subjectIndex = path.split("-").indexOf("subject")+1
                  //                    if(subjectIndex==2){
                  //                      result = getSplitInfo(path,subjectIndex+2)
                  //                    }else {
                  //                      result = getSplitInfo(path,subjectIndex+1)
                  //                    }
                  //                  }else if(path.contains("-tag-")){
                  //                    // 取最后一次出现的tag的位置信息
                  //                    val tagIndex = path.split("-").lastIndexOf("tag")+1
                  //                    result = getSplitInfo(path,tagIndex+1)
                  //                  }
                  result = getSubjectCodeByPath(path, "moretv")
                }
              }
            }
          }
        }
      }
    }
    result
  }


  /**
    * 该函数用于获取moretv的subjectCode
    */
  def getSubjectCode(path: String) = {
    var result: String = null
    if (path != null) {
      val regex = """^.*-([movie|tv|comic|zongyi|kids|hot|jilu]+[0-9]+).*$""".r
      path match {
        case regex(subjectCode) => result = subjectCode
        case _ =>
      }
    }
    result
  }


  /**
    * 该函数用于获取medusa的path中各级路径中的信息
    * index:用于指定一级、二级信息，在path中，一级、二级路径是通过"-"来分隔,index取值为：1,2,3...
    * subIndex:用于指定具体的某一级路径（如第一级）中的第几个字段的信息，不同字段的信息是通过"*"来分隔的,subIndex取值为：1,2,3...
    */

  def getPathMainInfo(x: String, index: Int, subIndex: Int) = {
    var result: String = null
    //过滤null的信息
    if (x != null) {
      //判断是需要哪一级路径
      if (index >= 2) {
        //获取非第一级的信息
        if (x.contains("-")) {
          val splitData = x.split("-")
          if (splitData.length >= index) {
            val indexPath = splitData(index - 1)
            result = getMedusaPathDetailInfo(indexPath, subIndex)
          }
        }
      } else {
        //直接获取第一级路径的信息
        if (x.contains("-")) {
          if (x != "-") {
            val firstPath = x.split("-")(0)
            result = getMedusaPathDetailInfo(firstPath, subIndex)
          }
        } else {
          //只有一级路径
          result = getMedusaPathDetailInfo(x, subIndex)
        }
      }
    }
    result
  }

  /**
    * 该函数用于获取medusa的一级、二级、...中第index个索引的信息，各个索引之间是用“*”来分隔的
    *
    * @param path     :路径中“-”拆分出来的每一级路径信息
    * @param subIndex ：路径中“-”拆分出来的每一级路径信息中，通过“*”拆分的第几个内容，从1,2，...开始
    * @return
    */
  def getMedusaPathDetailInfo(path: String, subIndex: Int) = {
    var result: String = null

    if (path != null) {
      if (subIndex >= 2) {
        /*所需信息为第一级路径中的其他字段信息*/
        if (path.contains("*")) {
          if (path.split("\\*").length >= subIndex) {
            result = path.split("\\*")(subIndex - 1)
          }
        }
      } else {
        /*所需信息为第一级路径中的第一个字段信息*/
        if (path.contains("*")) {
          if (path != "*") {
            try {
              result = path.split("\\*")(0)
            } catch {
              case e: Exception => e.printStackTrace()
            }
          }
        } else {
          result = path.toString
        }
      }
    }
    result
  }

  /**
    * 该函数用于获取moretv的分隔信息，moretv的path是用“-”来分隔的,index从1开始
    *
    * @param path
    * @param index
    * @return
    */
  def getSplitInfo(path: String, index: Int) = {
    var result: String = null
    if (path != null) {
      if (index >= 2) {
        /*所需信息为第一级路径中的其他字段信息*/
        if (path.contains("-")) {
          if (path.split("-").length >= index) {
            result = path.split("-")(index - 1)
          }
        }
      } else {
        /*所需信息为第一级路径中的第一个字段信息*/
        if (path.contains("-")) {
          if (path != "-") {
            result = path.split("-")(0)
          }
        } else {
          result = path.toString
        }
      }
    }
    result
  }

  /**
    * 从路径中获取专题code
    */
  def getSubjectCodeByPath(path: String, flag: String) = {
    var result: String = null
    if (flag != null) {
      flag match {
        case "medusa" => {
          if (path != null) {
            if (path.contains("subject")) {
              val subjectCode = SubjectUtils.getSubjectCode(path)
              if (subjectCode != null) {
                result = subjectCode
              }
            }
          }
        }
        case "moretv" => {
          if (path != null) {
            val info = SubjectUtils.getSubjectCodeAndPath(path)
            if (!info.isEmpty) {
              val subjectCode = info(0)
              result = subjectCode._1
            }
          }
        }
        case _ =>
      }
    }
    result
  }


  /**
    * 从路径中获取专题code
    */
  /* will delete
    def getSubjectCodeByPathETL(path: String, flag: String) = {
      var result: String = null
      if (flag != null && path != null) {
        flag match {
          case "medusa" => {
            if ("subject".equalsIgnoreCase(getPathMainInfo(path, 1, 1))) {
              val subjectCode = MedusaSubjectNameCodeUtils.getSubjectCode(path)
              if (!" ".equalsIgnoreCase(subjectCode)) {
                result = subjectCode
              }
            }
          }
          case "moretv" => {
            val info = SubjectUtils.getSubjectCodeAndPath(path)
            if (!info.isEmpty) {
              val subjectCode = info(0)
              result = subjectCode._1
            }
          }
          case _ =>
        }
      }
      result
    }
  */


  /**
    * 从路径中获取专题名称,对于medusa日志，可以从pathSpecial解析出subjectName；对于moretv日志，日志里面不存在subjectName打点
    *
    * will delete and get it from SubjectUtils
    * def getSubjectNameByPathETL(pathSpecial: String): String = {
    * var result: String = null
    * if (pathSpecial != null) {
    * if ("subject".equalsIgnoreCase(getPathMainInfo(pathSpecial, 1, 1))) {
    * val subjectCode = SubjectUtils.getSubjectCode(pathSpecial)
    * val pathLen = pathSpecial.split("-").length
    * if (pathLen == 2) {
    * result = getPathMainInfo(pathSpecial, 2, 1)
    * } else if (pathLen > 2) {
    * var tempResult = getPathMainInfo(pathSpecial, 2, 1)
    * if (subjectCode != " ") {
    * for (i <- 2 until pathLen - 1) {
    * tempResult = tempResult.concat("-").concat(getPathMainInfo(pathSpecial, i + 1, 1))
    * }
    * result = tempResult
    * } else {
    * for (i <- 2 until pathLen) {
    * tempResult = tempResult.concat("-").concat(getPathMainInfo(pathSpecial, i + 1, 1))
    * }
    * result = tempResult
    * }
    * }
    * }
    * }
    * result
    * }*/

  def getSubjectTypeByPathETL(path: String, flag: String): String = {
    var result: String = null
    if (flag != null && path != null) {
      flag match {
        case "medusa" => {
          result = getPathMainInfo(path, 1, 1)
          if (result.equalsIgnoreCase("subject")) {
            result = "subject"
          }
        }
        case "moretv" => {
          if (path.contains("-subject-")) {
            result = "subject"
          }
        }
        case _ =>
      }
    }
    result
  }

  /**
    * 获取4X直播播放记录的大首页入口
    *
    * @param path
    * @return
    */
  def getLiveLauncherEntrance4X(path: Seq[Row]): String = {
    if (path == null) null
    else if (path(0).getAs[String](ACCESS_AREA) == "editor_recommend"
      && path(0).getAs[String](LINK_VALUE) != "launHistory") "4.0首页今日推荐"
    else if (path(0).getAs[String](ACCESS_AREA) == "editor_Live") "首页LIVE板块"
    else if (HISTORY_COLLECT_LIST.contains(path(0).getAs[String](LINK_VALUE))) "历史收藏"
    else "其他"
  }

  /**
    * 获取3X直播播放记录的大首页入口
    *
    * @param path
    * @return
    * //    */
  def getLiveLauncherEntrance3X(path: String): String = {
    if (path == null) null
    else if (path.contains("home*recommendation")) "首页今日推荐"
    else if (path.contains("home*live")) "首页LIVE板块"
    else if (path.contains("home*my_tv*live")) "自定义入口"
    else if (path.contains("history_collect")) "历史收藏"
    else "其他"
  }


  /*
    //medusa 列表页入口  少儿 正则表达式
    private val regex_medusa_list_category_kids = ("home\\*(classification|my_tv)\\*kids-kids_home-([0-9A-Za-z_-]+)\\*([a-zA-Z0-9-\\u4e00-\\u9fa5]+)").r
    private val regex_medusa_list_category_kids2 = ("(kids_home)-([0-9A-Za-z_]+)\\*([·a-zA-Z0-9-\\u4e00-\\u9fa5]+)").r

    //private val regex_medusa_list_category_kids3 =("home\\*(classification|my_tv)\\*kids-kids_home-([0-9A-Za-z_]+)-(search)\\*([a-zA-Z0-9-\\u4e00-\\u9fa5]+)").r
    private val regex_medusa_list_category_kids3 = ("(.*kids_home)-([0-9A-Za-z_]+)-(search)\\*([a-zA-Z0-9-\\u4e00-\\u9fa5]+)").r

    //medusa 列表页入口  音乐 正则表达式
    //private val regex_medusa_list_category_mv = ("home\\*(classification|my_tv)\\*mv-mv\\*([a-zA-Z0-9_\\u4e00-\\u9fa5]+\\*[a-zA-Z0-9_\\u4e00-\\u9fa5]+)[-]?([a-zA-Z0-9_\\u4e00-\\u9fa5]*[\\*]?[a-zA-Z0-9_\\u4e00-\\u9fa5]*)").r

    //medusa 列表页入口  体育 正则表达式
    private val jianshen = "瑜伽健身|情侣健身|增肌必备|快速燃脂"
    private val dzjj = "英雄联盟|穿越火线|王者荣耀|NEST"
    private val regex_medusa_list_category_sport = (s"home\\*(classification|my_tv)\\*[0-9]+-sports\\*League\\*(dj|dzjj|CBA|ozb|ouguan|yc|jianshen|olympic)-league\\*(赛事回顾|热点新闻|精彩专栏|直播赛程|HPL|${jianshen}|${dzjj})").r

    //medusa 列表页入口  其他简单类型 正则表达式
    // home*classification*jilu-jilu*前沿科技
    private val MEDUSA_LIST_PAGE_LEVEL_1_REGEX = UDFConstantDimension.MEDUSA_LIST_Page_LEVEL_1.mkString("|")

    //需要数组来解析 'home*classification*comic-comic*二次元*电台' 错误格式
    private val MEDUSA_LIST_PAGE_LEVEL_2_REGEX = UDFConstantDimension.MedusaPageDetailInfoFromSite.filter(!_.contains("*")).mkString("|")

    private val regex_medusa_list_category_other = (s"home\\*(classification|my_tv)\\*($MEDUSA_LIST_PAGE_LEVEL_1_REGEX)-($MEDUSA_LIST_PAGE_LEVEL_1_REGEX)\\*([a-zA-Z0-9&\u4e00-\u9fa5]+)").r
    private val regex_medusa_list_category_other_short = (s"($MEDUSA_LIST_PAGE_LEVEL_1_REGEX)\\*([a-zA-Z0-9&\u4e00-\u9fa5]+)").r
    private val regex_medusa_list_retrieval = (s"home\\*(classification|my_tv|live\\*eagle)\\*($MEDUSA_LIST_PAGE_LEVEL_1_REGEX)-($MEDUSA_LIST_PAGE_LEVEL_1_REGEX)[-*]?(${UDFConstantDimension.RETRIEVAL_DIMENSION}|${UDFConstantDimension.RETRIEVAL_DIMENSION_CHINESE}).*").r
    private val regex_medusa_list_retrieval_short = (s"($MEDUSA_LIST_PAGE_LEVEL_1_REGEX)[-*]?(${UDFConstantDimension.RETRIEVAL_DIMENSION}|${UDFConstantDimension.RETRIEVAL_DIMENSION_CHINESE}).*").r
    private val regex_medusa_list_search = (s"home\\*(classification|my_tv)\\*($MEDUSA_LIST_PAGE_LEVEL_1_REGEX)-($MEDUSA_LIST_PAGE_LEVEL_1_REGEX)[-*]?(${UDFConstantDimension.SEARCH_DIMENSION}|${UDFConstantDimension.SEARCH_DIMENSION_CHINESE}).*").r
    private val regex_medusa_list_search_short = (s"($MEDUSA_LIST_PAGE_LEVEL_1_REGEX)[-*]?(${UDFConstantDimension.SEARCH_DIMENSION}|${UDFConstantDimension.SEARCH_DIMENSION_CHINESE}).*").r
    private val regex_moretv_filter = (".*multi_search-(hot|new|score)-([\\S]+?)-([\\S]+?)-(all|qita|[0-9]+[-0-9]*)").r
    //private val regex_moretv_filter = (".*multi_search-(hot|new|score)-([\\S]+?)-([\\S]+?)-(.*)").r
    private val regex_medusa_filter = (".*retrieval\\*(hot|new|score)\\*([\\S]+?)\\*([\\S]+?)\\*(all|qita|[0-9]+[\\*0-9]*)").r

    //用于频道分类入口统计，解析出资讯的一级入口、二级入口
    private val regex_medusa_recommendation = (s"home\\*recommendation\\*[\\d]{1}-($MEDUSA_LIST_PAGE_LEVEL_1_REGEX)\\*(.*)").r


    /*获取列表页入口信息
     第一步，过滤掉包含search字段的pathMain
     第二步，判别是来自classification还是来自my_tv
     第三步，分音乐、体育、少儿以及其他类型【电视剧，电影等】获得列表入口信息,根据具体的分类做正则表达*/
    def getListCategoryMedusaETL(pathMain: String, index_input: Int): String = {
      var result: String = null
      if (null == pathMain) {
        result = null
      } else if (pathMain.contains(UDFConstantDimension.HORIZONTAL) || pathMain.contains(UDFConstantDimension.MV_RECOMMEND_HOME_PAGE) ||
        pathMain.contains(UDFConstantDimension.MV_TOP_HOME_PAGE) || pathMain.contains(UDFConstantDimension.HOME_SEARCH)

      /** 为了统计频道分类入口的 搜索 和 筛选 维度，注释掉 */
      //||pathMain.contains(UDFConstantDimension.RETRIEVAL_DIMENSION)
      ) {
        result = null
      } else if (pathMain.contains(UDFConstantDimension.HOME_CLASSIFICATION)
        || pathMain.contains(UDFConstantDimension.HOME_MY_TV)
        || pathMain.contains(UDFConstantDimension.HOME_LIVE_EAGLE)
        || pathMain.contains(UDFConstantDimension.KIDS_HOME)
        || pathMain.contains(UDFConstantDimension.SPORTS_LIST_DIMENSION_TRAIT)

        /** 为了统计频道分类入口的 搜索 和 筛选 维度，添加 */
        || pathMain.contains(UDFConstantDimension.RETRIEVAL_DIMENSION)
        || pathMain.contains(UDFConstantDimension.RETRIEVAL_DIMENSION_CHINESE)
        || pathMain.contains(UDFConstantDimension.SEARCH_DIMENSION)
        || pathMain.contains(UDFConstantDimension.SEARCH_DIMENSION_CHINESE)
        || pathMain.contains(UDFConstantDimension.HOME_RECOMMENDATION)
      ) {
        if (pathMain.contains("kids")) {
          result = KidsPathParserUtils.pathMainParse(pathMain, index_input)
        }
        /*少儿
        home*classification*kids-kids_home-kids_anim*动画专题    拆分出   kids_anim，动画专题
        home*classification*kids-kids_home-kandonghua*4-6岁     拆分出    kandonghua，4-6岁
        home*my_tv*kids-kids_home-kids_rhymes*儿歌热播*随便听听
        kids_home-kandonghua*探险·周,解析出main_category:kandonghua,sub_category:探险·周
        home*my_tv*kids-kids_home-kandonghua-search*ZZXZJ
        老版本：
        home-kids_home-kids_anim*热播推荐
        kids_anim*4-6岁

        if (pathMain.contains("kids")) {
          if (index_input == 1) {
            result = "kids"
          } else {
            val index = index_input
            regex_medusa_list_category_kids findFirstMatchIn pathMain match {
              case Some(p) => {
                result = p.group(index)
              }
              case None =>
            }

            regex_medusa_list_category_kids2 findFirstMatchIn pathMain match {
              case Some(p) => {
                result = p.group(index)
              }
              case None =>
            }

            regex_medusa_list_category_kids3 findFirstMatchIn pathMain match {
              case Some(p) => {
                result = p.group(index)
              }
              case None =>
            }
          }
        }*/

        /* 音乐
          home*classification*mv-mv*电台*电台  拆分出   电台*电台
          home*classification*mv-mv*mvCategoryHomePage*site_mvstyle-mv_category*电子
          拆分出  一级：mvCategoryHomePage*site_mvstyle ，二级：mv_category*电子
          home*my_tv*mv-mv*mvCategoryHomePage*site_mvarea-mv_category*港台
        */
        /* regex_medusa_list_category_mv findFirstMatchIn  path match {
           case Some(p) =>  {
             result=p.group(index)
           }
           case None => null
         }*/
        else if (pathMain.contains("mv-mv")) {
        }
        else if (pathMain.contains(UDFConstantDimension.SPORTS_LIST_DIMENSION_TRAIT)) {
          result = SportsPathParserUtils.pathMainParse(pathMain, index_input)
        }

        /**
          * 拆分出筛选维度,为了统计频道分类入口
          * home*classification*movie-movie-retrieval*hot*xiju*gangtai*all
          * home*my_tv*tv-tv-retrieval*hot*xiju*neidi*2000*2009
          * movie-retrieval*hot*xiju*gangtai*all
          * home*live*eagle-movie-retrieval*hot*kehuan*meiguo*all
          * home*classification*movie-movie*筛选
          * home*my_tv*movie-movie*筛选
          **/
        else if (pathMain.contains(UDFConstantDimension.RETRIEVAL_DIMENSION) || pathMain.contains(UDFConstantDimension.RETRIEVAL_DIMENSION_CHINESE)) {
          regex_medusa_list_retrieval findFirstMatchIn pathMain match {
            case Some(p) => {
              if (index_input == 1) {
                result = p.group(3)
              } else if (index_input == 2) {
                result = UDFConstantDimension.RETRIEVAL_DIMENSION_CHINESE
              }
            }
            case None =>
          }

          /** home-movie-retrieval*hot*dongzuo*gangtai*qita
            * movie-retrieval*hot*dongzuo*gangtai*qita
            * */
          regex_medusa_list_retrieval_short findFirstMatchIn pathMain match {
            case Some(p) => {
              if (index_input == 1) {
                result = p.group(1)
              } else if (index_input == 2) {
                result = UDFConstantDimension.RETRIEVAL_DIMENSION_CHINESE
              }
            }
            case None =>
          }
        }

        /**
          * 拆分出搜索维度，为了统计频道分类入口
          * home*classification*tv-tv-search*SHALA
          * home*my_tv*tv-tv-search*DQD
          * tv-search*SHALA
          * home*my_tv*movie-movie*搜索
          * home*classification*movie-movie*搜索
          **/
        else if (pathMain.contains(UDFConstantDimension.SEARCH_DIMENSION) || pathMain.contains(UDFConstantDimension.SEARCH_DIMENSION_CHINESE)) {
          regex_medusa_list_search findFirstMatchIn pathMain match {
            case Some(p) => {
              if (index_input == 1) {
                result = p.group(3)
              } else if (index_input == 2) {
                result = UDFConstantDimension.SEARCH_DIMENSION_CHINESE
              }
            }
            case None =>
          }

          /**
            * home-movie-search*SHENDENG
            * movie-search*SHENDENG
            **/
          regex_medusa_list_search_short findFirstMatchIn pathMain match {
            case Some(p) => {
              if (index_input == 1) {
                result = p.group(1)
              } else if (index_input == 2) {
                result = UDFConstantDimension.SEARCH_DIMENSION_CHINESE
              }
            }
            case None =>
          }
        }

        /**
          *
          * home*recommendation*1-hot*今日焦点 解析出 hot,今日焦点
          **/
        else if (pathMain.contains(UDFConstantDimension.HOME_RECOMMENDATION)) {
          regex_medusa_recommendation findFirstMatchIn pathMain match {
            case Some(p) => {
              if (index_input == 1) {
                result = p.group(1)
              } else if (index_input == 2) {
                result = p.group(2)
              }
            }
            case None =>
          }
        }

        /** 其他频道，例如 电影，电视剧
          * home*classification*jilu-jilu*前沿科技
          * home*classification*movie-movie*动画电影
          * home*classification*tv-tv*电视剧专题
          * home*my_tv*account-accountcenter_home*节目预约

          */
        else {
          regex_medusa_list_category_other findFirstMatchIn pathMain match {
            case Some(p) => {
              if (index_input == 1) {
                result = p.group(3)
              } else if (index_input == 2) {
                result = p.group(4)
              }
            }
            case None => {
              /**
                * pathMain='movie*院线大片' 在线上统计逻辑忽略，在数仓正则里也忽略
                **/
              regex_medusa_list_category_other_short findFirstMatchIn pathMain match {
                case Some(p) => {
                  if (index_input == 1) {
                    result = p.group(1)
                  } else if (index_input == 2) {
                    result = p.group(2)
                  }
                }
                case None =>
              }
            }
          }
        }
      }
      result
    }

    /**
      * 2.x，原有统计分析没有做少儿；体育最新的逻辑解析没有上线
      * SportsPathParserUtils现在没有解析2.x path路径
      *
      * */
    def getListCategoryMoretvETL(path: String, index_input: Int): String = {
      var result: String = null
      if (null != path) {
        //少儿使用最新逻辑
        if(path.contains("kids")){
          result=KidsPathParserUtils.pathParse(path,index_input)
        }else {
          //其他类型仍然使用原有逻辑
          if (index_input == 1) {
            result = getSplitInfo(path, 2)
            if (result != null) {
              // 如果accessArea为“navi”和“classification”，则保持不变，即在launcherAccessLocation中
              if (!UDFConstantDimension.MoretvLauncherAccessLocation.contains(result)) {
                // 如果不在launcherAccessLocation中，则判断accessArea是否在uppart中
                if (UDFConstantDimension.MoretvLauncherUPPART.contains(result)) {
                  result = "MoretvLauncherUPPART"
                } else {
                  result = null
                }
              }
            }
          }else if (index_input == 2) {
            result = getSplitInfo(path, 3)
            if (result != null) {
              if (getSplitInfo(path, 2) == "search") {
                result = ""
              }
              if (getSplitInfo(path, 2) == "kids_home" || getSplitInfo(path, 2) == "sports") {
                result = getSplitInfo(path, 3) + "-" + getSplitInfo(path, 4)
              }
              if (!UDFConstantDimension.MoretvPageInfo.contains(getSplitInfo(path, 2))) {
                result = null
              }
            }
          }
        }
      }
      result
    }*/


  def main(args: Array[String]) {
    //val pathMain = "home*live*eagle-movie-retrieval*hot*kehuan*meiguo*all"
    //val pathMain = "home*my_tv*movie-movie*筛选"
    /*   val pathMain = "kandonghua*ab-kandonghua*ab-kandonghua*ab1"
       val qqq = (s".*-(.*)\\*(.*)").r
       qqq findFirstMatchIn pathMain match {
         case Some(p) => {
           println(p.group(1))
           println(p.group(2))
         }
         case None =>
       }*/
    //val pathMain = "home*my_tv*movie-movie*搜索"
    //val pathMain = "home*classification*movie-movie*搜索"
    //val pathMain = "home*classification*movie-movie*筛选"
    //val pathMain = "movie-retrieval*hot*xiju*gangtai*all"
    //val pathMain = "home-movie-retrieval*hot*dongzuo*gangtai*qita"
    //val pathMain = "home*my_tv*movie-movie*搜索"
    //println(pathMain)
    //val pathMain = "home*classification*mv-mv*电台*电台"
    //val pathMain = "home*live*eagle-movie*院线大片"
    //println(MEDUSA_LIST_PAGE_LEVEL_2_REGEX)
    var result: String = null
    val subjectCode = new String("bb") //MedusaSubjectNameCodeUtils.getSubjectCode(path)


    if (subjectCode != "bb") {
      result = subjectCode
      println("a")
    } else {
      println("ccc")
    }
    println(result)

    /* println(PathParser.getListCategoryMedusaETL(pathMain, 1))
     println(PathParser.getListCategoryMedusaETL(pathMain, 2))*/
  }
}

