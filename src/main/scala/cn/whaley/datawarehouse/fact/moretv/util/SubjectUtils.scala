package cn.whaley.datawarehouse.fact.moretv.util

import java.sql.Struct

import breeze.linalg.split
import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition}
import cn.whaley.datawarehouse.global.LogConfig
import com.alibaba.fastjson.JSON
import org.apache.spark.sql.Row

import scala.util.control.Breaks.{break, breakable}


/**
  * Created by michael on 2017/4/24.
  * 收集所有关于专题的工具类到此类中
  */
object SubjectUtils extends LogConfig{

  /**
    * 从路径中获取专题code
    */
  def getSubjectCodeByPathETL(pathSpecial: String, path: String, path_json: Seq[Row], flag: String): String = {
    var result: String = null
    flag match {
      case MEDUSA => {
        if (SUBJECT.equalsIgnoreCase(PathParserUtils.getPathMainInfo(pathSpecial, 1, 1))) {
          result = getSubjectCode(pathSpecial)
        }
      }
      case MORETV => {
        val info = getSubjectCodeAndPath(path)
        if (info.nonEmpty) {
          val subjectCode = info(0)
          result = subjectCode._1
        }
      }
      case MEDUSA4X => {
        result = getSubjectCodeFromPath4x(path_json)
      }
      case UTVMORE => { //优视猫的路径和电视猫4x是一致的，所以这里采取一样的逻辑
        result = getSubjectCodeFromPath4x(path_json)
      }
      case _ =>
    }

    result
  }

  //for main3x
  def getSubjectCodeByPathETL(pathSpecial:String, path: String ,flag: String) :String= {
    getSubjectCodeByPathETL(pathSpecial,path,null,flag)
  }

  /**
    * 从4x路径中解析出组合专题中的子专题
    * @param path_4x
    * @return
    */
  def getSubSubjectCodeByPath4xETL(path_4x: Seq[Row]) :String= {
    var result: String = null
    val len = path_4x.length - 1
    if ( path_4x(len).getAs[String]("page_type") == "GroupSubjectActivity"){
      result = path_4x(len).getAs[String]("sub_subject_code")
    }
    result
  }

  private val regex_etl ="""([a-zA-Z_]+)([0-9]+)""".r
  //private val regex_etl="""(movie|tv|hot|kids|zongyi|comic|jilu|sports|xiqu|mv)([0-9]+)""".r
  //private val regex_etl="""(movie|tv|hot|kids|zongyi|comic|jilu|sports|xiqu|mv|kid)([0-9]+)""".r
  private val regexSubjectName="""subject-([a-zA-Z0-9-\u4e00-\u9fa5]+)""".r
  // 获取专题code
  def getSubjectCode(subject:String) = {
    var subjectCode: String = null
    if (subject != null){
      regex_etl findFirstMatchIn subject match {
        // 如果匹配成功，说明subject包含了专题code，直接返回专题code
        case Some(m) => {
          subjectCode = m.group(1) + m.group(2)
        }
        case None =>
      }
    }
    subjectCode
  }

  def getSubjectEntranceByPath4xETL(path_4x: Seq[Row]): String ={
    var result: String = null
    var entrance: Row = null
    /** 取得专题上一级页面信息 **/
    if(getSubjectCodeFromPath4x(path_4x) != null){
      //专题--专题--详情页--详情页--播放
      if(path_4x.length >= 8 && path_4x(path_4x.length - 4).getAs[String]("page_type") == "SubjectHomeActivity" && path_4x(path_4x.length - 6).getAs[String]("page_type") == "SubjectHomeActivity"){
        entrance = path_4x(path_4x.length - 8)
      }
      //专题--专题--详情页--播放
      else if(path_4x.length >= 7 && path_4x(path_4x.length - 3).getAs[String]("page_type") == "SubjectHomeActivity" && path_4x(path_4x.length - 5).getAs[String]("page_type") == "SubjectHomeActivity"){
        entrance = path_4x(path_4x.length - 7)
      }
      //专题--详情页--详情页--播放
      else if(path_4x.length >= 6 && path_4x(path_4x.length - 4).getAs[String]("page_type") == "SubjectHomeActivity"){
        entrance = path_4x(path_4x.length - 6)
      }
       //专题--详情页--详情页（低危） 或 专题--详情页--播放
      else if(path_4x.length >= 5 && path_4x(path_4x.length - 3).getAs[String]("page_type") == "SubjectHomeActivity"){
        entrance = path_4x(path_4x.length - 5)
      }
        //专题--详情页（低危）
      else if(path_4x.length >= 4 && path_4x(path_4x.length - 2).getAs[String]("page_type") == "SubjectHomeActivity"){
        entrance = path_4x(path_4x.length - 4)
      }
        //短视频专题
      else if(path_4x.length >= 3 && (path_4x(path_4x.length - 1).getAs[String]("page_type") == "SubjectHomeActivity" || path_4x(path_4x.length - 1).getAs[String]("page_type") == "GroupSubjectActivity")){
        entrance = path_4x(path_4x.length - 3)
      }
       //add by huganglong 190318
       //小视频专题
      else if(path_4x.length >= 2 && path_4x(path_4x.length - 1).getAs[String]("page_type") == "SmallVideoActivity"){
        entrance = path_4x(path_4x.length - 2)
      }
    }
    /** 解析专题上一级页面（来源页面） **/
    var entrancePage: String = ""
    if (entrance != null) {
      entrancePage = entrance.getAs[String]("page_type")
    }
    /** 获取treeSite **/
    var treeSite:String = ""
    breakable {
      if (entrance != null) {
        if (entrance.getAs[String]("page_url") != null) {
          entrance.getAs[String]("page_url").split("&").foreach(url => {
            val kvs = url.split("=")
            if (kvs(0) == "treeSite" && kvs.length > 1) {
              treeSite = kvs(1)
              break()
            }
          })
        }
      }
    }
    /** 获取channelType **/
    var channelType:String = ""
    breakable{
      if(entrance != null) {
        if (entrance.getAs[String]("page_url") != null) {
          entrance.getAs[String]("page_url").split("&").foreach(url => {
            val kvs = url.split("=")
            if (kvs(0) == "channelType" && kvs.length > 1) {
              channelType = kvs(1)
              break()
            }
          })
        }
      }
    }
    if(entrancePage == "LauncherActivity" || entrancePage == "DetailHomeActivity" || entrancePage == "KidsHomeActivity"){
      result = entrancePage
    }
    else if(entrancePage == "VodActivity" && treeSite.contains("_")){
      result = channelType + "*" + entrancePage + "*" +  "common_site"
    }
    else if ((entrancePage == "KidsAnimActivity" || entrancePage == "KidsRhymesActivity"||entrancePage == "KidsCollectActivity")  && treeSite.contains("_")){
      result = channelType + "*" + entrancePage + "*" +  "kids_site"
    }

    else if(entrancePage == "VodActivity" && treeSite.length == 4){
      result = channelType +"*"+ "specialArea"
    }

    else if(entrancePage == "RecHomeActivity"){
      result = entrancePage + channelType
    }
    else if (entrancePage == "SearchActivity") {
      result = entrancePage
    }

    //    if(entrancePage == "LauncherActivity" && entrance.getAs[String]("access_area")=="editor_recommend"){
    //      result = "LauncherActivity" +"*" +"editor_recommend"
    //    }
    //    else if (entrancePage == "LauncherActivity" && entrance.getAs[String]("sub_access_area").contains("editor_funny")){
    //      result = "LauncherActivity" +"*" +"editor_funny"
    //    }
    //    else if(entrancePage == "LauncherActivity" && entrance.getAs[String]("sub_access_area").contains("editor_game")){
    //      result = "LauncherActivity" + "*" + "editor_game"
    //    }
    //    else if(entrancePage == "LauncherActivity" && entrance.getAs[String]("access_area") == "editor_plan"){
    //      result = "LauncherActivity" + "*" + "editor_plan"
    //    }
    //    else if(entrancePage == "LauncherActivity" && entrance.getAs[String]("access_area") == "editor_vip"){
    //      result = "LauncherActivity" + "*" + "editor_vip"
    //    }
    //    else if(entrancePage == "VodActivity"  && treeSite.contains("site_")){
    //      result = channelType + "*" +  entrance.getAs[String]("page_tab")
    //    }
    //    else if(entrancePage == "VodActivity" && treeSite.length == 4){
    //      result = channelType +"*"+ treeSite +"*"+ entrance.getAs[String]("page_tab")
    //    }
    //    else if(entrancePage == "KidsAnimActivity" || entrancePage == "KidsRhymesActivity"||entrancePage == "KidsCollectActivity"|| entrancePage == "KidsHomeActivity"|| entrancePage == "DetailHomeActivity"){
    //      result = entrancePage
    //    }
    //    else if(entrancePage == "RecHomeActivity"){
    //      result = "RecHomeActivity" +"*"+ channelType
    //    }
    result
  }



  /*例子：假设pathSpecial为subject-儿歌一周热播榜,解析出 儿歌一周热播榜 */
  def getSubjectNameETL(subject:String) :String= {
    regexSubjectName findFirstMatchIn subject match {
      // 如果匹配成功，说明subject包含了专题名称，直接返回专题名称
      case Some(m) => {
        m.group(1)
      }
      case None => null
    }
  }

  /**
    * 从路径中获取专题名称,对于medusa日志，可以从pathSpecial解析出subjectName；对于moretv日志，日志里面不存在subjectName打点
    *
    * @param pathSpecial medusa play pathSpecial field
    * @return subject_name string value or null
    *         Example:
    *
    *         {{{
    *                sqlContext.sql("
    *                select pathSpecial,subjectName,subjectCode
    *                from log_data
    *                where flag='medusa' and pathSpecial is not null and size(split(pathSpecial,'-'))=2").show(100,false)
    *         }}}
    **/

  def getSubjectNameByPathETL(pathSpecial: String): String = {
    var result: String = null
    if (pathSpecial != null) {
      if (SUBJECT.equalsIgnoreCase(PathParserUtils.getPathMainInfo(pathSpecial, 1, 1))) {
        val subjectCode = SubjectUtils.getSubjectCode(pathSpecial)
        val pathLen = pathSpecial.split("-").length
        if (pathLen == 2) {
          result = PathParserUtils.getPathMainInfo(pathSpecial, 2, 1)
        } else if (pathLen > 2) {
          var tempResult = PathParserUtils.getPathMainInfo(pathSpecial, 2, 1)
          if (subjectCode != null) {
            for (i <- 2 until pathLen - 1) {
              tempResult = tempResult.concat("-").concat(PathParserUtils.getPathMainInfo(pathSpecial, i + 1, 1))
            }
            result = tempResult
          } else {
            for (i <- 2 until pathLen) {
              tempResult = tempResult.concat("-").concat(PathParserUtils.getPathMainInfo(pathSpecial, i + 1, 1))
            }
            result = tempResult
          }
        }
      }
    }
    result
  }

  /** 通过专题subject_code and subject_name获得subject_sk  */
  def getSubjectSK(): DimensionColumn = {
    new DimensionColumn("dim_medusa_subject",
      List(DimensionJoinCondition(
        Map("subjectCode" -> "subject_code"),
        null, null, "subjectCode is not null"
      ),
        DimensionJoinCondition(  //先按照名字和contentType关联，防止出现不同频道重名的专题
          Map("subjectName" -> "subject_name", "contentType" -> "subject_content_type"),
          null, null, "subjectName is not null"
        ),
        DimensionJoinCondition(
          Map("subjectName" -> "subject_name"),
          null, null, "subjectName is not null"
        )
      ),
      "subject_sk")
  }

  //匹配首页上的专题
  val regexSubjectA = "home-(hotrecommend)(-\\d+-\\d+)?-(hot\\d+|movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|sports\\d+|mv\\d+|xiqu\\d+)".r
  //匹配首页上的专题套专题
  val regexSubjectA2 = ("home-(hotrecommend)(-\\d+-\\d+)?-(hot\\d+|movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|sports\\d+|mv\\d+|xiqu\\d+)-"
    + "(actor|hot\\d+|movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|sports\\d+|mv\\d+|xiqu\\d+)").r
  //匹配在三级页面的专题
  val regexSubjectB = "home-(movie|zongyi|tv|comic|kids|jilu|hot|sports|mv|xiqu)-(\\w+)-(movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|hot\\d+|sports\\d+|mv\\d+|xiqu\\d+)".r
  //匹配在三级页面的专题套专题
  val regexSubjectB2  = ("home-(movie|zongyi|tv|comic|kids|jilu|hot|mv|xiqu)-(\\w+)-(movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|hot\\d+|mv\\d+|xiqu\\d+)-"
    + "(actor|movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|hot\\d+|mv\\d+|xiqu\\d+)").r
  //匹配第三方跳转的专题
  val regexSubjectC = "(thirdparty_\\d{1})[\\w\\-]+-(movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|hot\\d+|sports\\d+|mv\\d+|xiqu\\d+)".r
  //匹配少儿毛推荐的专题
  val regexSubjectD = "home-kids_home-(\\w+)-(kids\\d+)".r
  //匹配少儿三级页面中的专题
  val regexSubjectE = "home-kids_home-(\\w+)-(\\w+)-(kids\\d+)".r
  //匹配历史收藏中的专题
  val regexSubjectF = "home-(history)-[\\w\\-]+-(movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|jilu\\d+|hot\\d+|sports\\d+|mv\\d+|xiqu\\d+)".r
  //匹配历史收藏中的专题套专题
  val regexSubjectF2 = "home-(history)-[\\w\\-]+-(movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|jilu\\d+|hot\\d+|sports\\d+|mv\\d+|xiqu\\d+)-(actor|movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|jilu\\d+|hot\\d+|sports\\d+|mv\\d+|xiqu\\d+)".r
  //暂时不清楚是匹配哪种情况，暂且保留此匹配项
  val regexSubjectG = "home-(movie|zongyi|tv|comic|kids|jilu|hot)-(movie\\d+|zongyi\\d+|tv\\d+|comic\\d+|kids\\d+|jilu\\d+|hot\\d+|sports\\d+)".r

  def getSubjectCodeAndPath(path:String) = {
    var result: List[(String, String)] = List()
    if (path != null){
      regexSubjectA2 findFirstMatchIn path match {
        case Some(a2) => result = (a2.group(4),a2.group(1))::(a2.group(3),a2.group(1))::Nil
        case None => regexSubjectA findFirstMatchIn path match {
          case Some(a) => result = (a.group(3),a.group(1))::Nil
          case None => regexSubjectB2 findFirstMatchIn path match {
            case Some(b2) => result = (b2.group(3),b2.group(2))::(b2.group(4),b2.group(2))::Nil
            case None => regexSubjectB findFirstMatchIn path match {
              case Some(b) => result = (b.group(3),b.group(2))::Nil
              case None => regexSubjectC findFirstMatchIn path match {
                case Some(c) => result = (c.group(2),c.group(1))::Nil
                case None => regexSubjectD findFirstMatchIn path match {
                  case Some(d) => result = (d.group(2),d.group(1))::Nil
                  case None => regexSubjectE findFirstMatchIn path match {
                    case Some(e) => result = (e.group(3),e.group(2))::Nil
                    case None => regexSubjectF2 findFirstMatchIn path match {
                      case Some(f2) => result = (f2.group(2),f2.group(1))::(f2.group(3),f2.group(1))::Nil
                      case None => regexSubjectF findFirstMatchIn path match {
                        case Some(f) => result = (f.group(2),f.group(1))::Nil
                        case None => regexSubjectG findFirstMatchIn path match {
                          case Some(g) => result = (g.group(2),g.group(1))::Nil
                          case None => Nil
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    result
  }

  def getSubjectCodeFromPath4x(path_4x:Seq[Row]) :String={
    var result:String = null
    //如果path为空，则返回null
    if (path_4x == null || path_4x.isEmpty) return result
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
      result = path_4x(len-2).getAs[String]("sub_subject_code")
    }
    else if  (one_from_end == "PlayActivity" && second_from_end == "DetailHomeActivity"  && third_from_end == "DetailHomeActivity" && fourth_from_end == "SubjectHomeActivity"){
      result = path_4x(len-3).getAs[String]("sub_subject_code")
    }
    //长视频专题(低配)
    else if  (one_from_end == "DetailHomeActivity"  && second_from_end == "DetailHomeActivity" && third_from_end == "SubjectHomeActivity"){
      result = path_4x(len-2).getAs[String]("sub_subject_code")
    }
    else if  (one_from_end == "DetailHomeActivity"  && second_from_end == "SubjectHomeActivity"){
      result = path_4x(len-1).getAs[String]("sub_subject_code")
    }
    //短视频组合专题，取子专题
    else if (one_from_end == "GroupSubjectActivity"){
      result = path_4x(len).getAs[String]("sub_subject_code")
    }
    //短视频其他通用专题
    else if (one_from_end == "SubjectHomeActivity"){
      result = path_4x(len).getAs[String]("sub_subject_code")
    }
    //add by huganglong 190318
    //小视频专题
    else if (one_from_end == "SmallVideoActivity"){
      val pageUrl = path_4x(len).getAs[String]("page_url")
      if(pageUrl != null){
        val splits = pageUrl.split("&")
        for(s <- splits){
          if(s!=null && "linkValue".equals(s.split("=")(0))){
            result = s.split("=")(1)
          }
        }
      }
    }
    result
  }

  def getSubjectCodeAndPathWithId(path:String,userId:String) = {
    getSubjectCodeAndPath(path).map(x => (x,userId))
  }


}