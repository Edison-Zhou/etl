package cn.whaley.datawarehouse.fact.moretv

import java.util.Calendar

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition, UserDefinedColumn}
import cn.whaley.datawarehouse.fact.FactEtlBase
import cn.whaley.datawarehouse.fact.moretv.util._
import cn.whaley.datawarehouse.global.LogConfig
import cn.whaley.datawarehouse.util._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
  * Created by michel on 17/4/24.
  */
object PlayFinal extends FactEtlBase with  LogConfig{
  /** log type name */
  topicName = "fact_medusa_play"

  source = "main3x"

  partition = 1000

//  uniqueLogIdColumn = "logid"

  /**
    * step 1, get data source
    * */
  override def readSource(startDate: String, startHour: String): DataFrame = {
  //示例  spark.sqlContext.udf.register("subjectPathParse",(x:String,flag:String) => x.toUpperCase)
    /*println("------- before readSource "+Calendar.getInstance().getTime)
    val date = DateUtils.addDays(DateFormatUtils.readFormat.parse(startDate), 1)
    val reallyStartDate=DateFormatUtils.readFormat.format(date)
    //    val medusa_input_dir = DataExtractUtils.getParquetPath(LogPath.MEDUSA_PLAY, reallyStartDate)
    //    val moretv_input_dir = DataExtractUtils.getParquetPath(LogPath.MORETV_PLAYVIEW, reallyStartDate)
    //    val medusaFlag = HdfsUtil.IsInputGenerateSuccess(medusa_input_dir)
    //    val moretvFlag = HdfsUtil.IsInputGenerateSuccess(moretv_input_dir)
    val medusaFlag = true
    val moretvFlag = true
    if (medusaFlag && moretvFlag) {
      //      val medusaDf = DataExtractUtils.readFromParquet(sqlContext, LogPath.MEDUSA_PLAY, reallyStartDate).withColumn("flag",lit(MEDUSA))
      //      val moretvDf = DataExtractUtils.readFromParquet(sqlContext, LogPath.MORETV_PLAYVIEW, reallyStartDate).withColumn("flag",lit(MORETV))
      val medusaDf = DataExtractUtils.readFromOdsParquet(sqlContext, "ods_view.log_medusa_main3x_play", startDate, startHour)
        .withColumn("flag", lit(MEDUSA))
      val moretvDf = DataExtractUtils.readFromOdsParquet(sqlContext, "ods_view.log_medusa_main20_playview", startDate, startHour)
        .withColumn("flag", lit(MORETV))

      val medusaDfCombine: DataFrame = Play3xCombineUtils.get3xCombineDataFrame(medusaDf, sqlContext)
      val medusaRDD: Dataset[String] =medusaDfCombine.toJSON

      val moretvDfFilter = Play2xFilterUtilsNew.get2xFilterDataFrame(moretvDf, sqlContext)
      val moretvRDD: Dataset[String] =moretvDfFilter.toJSON

      val mergerRDD=medusaRDD.union(moretvRDD)
      val mergerDataFrame: DataFrame = sqlContext.read.json(mergerRDD.rdd)
      Play3xCombineUtils.factDataFrameWithIndex.unpersist()
      mergerDataFrame

    }else{
      throw new RuntimeException("medusaFlag or moretvFlag is false")
    }*/

    val medusaDf = DataExtractUtils.readFromOdsParquet(sqlContext, "ods_view.log_medusa_main3x_play", startDate, startHour)
      .withColumn("flag", lit(MEDUSA))
        .withColumn("path", lit(""))
    Play3xCombineUtils.get3xCombineDataFrame(medusaDf, sqlContext)
  }

  /**
    * step 2, generate new columns
    * */
  addColumns = List(
    UserDefinedColumn("ipKey", udf(getIpKey: String => Long), List("realIP")),
    UserDefinedColumn("dim_date", udf(getDimDate: String => String), List("fDatetime")),
    UserDefinedColumn("dim_time", udf(getDimTime: String => String), List("fDatetime")),
//    UserDefinedColumn("app_series", udf(getAppSeries: String => String), List("version")),
//    UserDefinedColumn("app_version", udf(getAppVersion: String => String), List("version")),
    UserDefinedColumn("subjectCode", udf(SubjectUtils.getSubjectCodeByPathETL: (String, String, String) => String), List("pathSpecial", "path","flag")),
    UserDefinedColumn("subjectName", udf(SubjectUtils.getSubjectNameByPathETL: String => String), List("pathSpecial")),
    UserDefinedColumn("mainCategory", udf(ListCategoryUtils.getListMainCategory: (String,String,String) => String), List("pathMain", "path","flag")),
    UserDefinedColumn("secondCategory",udf(ListCategoryUtils.getListSecondCategory: (String,String,String) => String), List("pathMain", "path","flag")),
    UserDefinedColumn("thirdCategory", udf(ListCategoryUtils.getListThirdCategory: (String,String,String) => String), List("pathMain", "path","flag")),
    UserDefinedColumn("fourthCategory", udf(ListCategoryUtils.getListFourthCategory: (String,String,String) => String), List("pathMain", "path","flag")),
    UserDefinedColumn("launcherAreaCode", udf(EntranceTypeUtils.getEntranceAreaCode: (String, String, String) => String), List("pathMain", "path","flag")),
    UserDefinedColumn("launcherLocationCode", udf(EntranceTypeUtils.getEntranceLocationCode: (String, String,String) => String), List("pathMain", "path","flag")),
    UserDefinedColumn("launcherRecommendIndex", udf(EntranceTypeUtils.getRecommendLocationIndex: String => String), List("pathMain")),
    UserDefinedColumn("filterContentType", udf(FilterCategoryUtils.getFilterCategoryContentType: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("filterCategoryFirst", udf(FilterCategoryUtils.getFilterCategoryFirst: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("filterCategorySecond", udf(FilterCategoryUtils.getFilterCategorySecond: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("filterCategoryThird", udf(FilterCategoryUtils.getFilterCategoryThird: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("filterCategoryFourth", udf(FilterCategoryUtils.getFilterCategoryFourth: (String,String,String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("recommendSourceType", udf(RecommendUtils.getRecommendSourceType: (String,String,String) => String), List("pathSub", "path", "flag")),
    UserDefinedColumn("recommendLogType", udf(RecommendUtils.getRecommendLogType: (String,String,String) => String), List("pathSub", "path", "flag")),
    UserDefinedColumn("previousSid", udf(RecommendUtils.getPreviousSid: String => String), List("pathSub")),
    UserDefinedColumn("previousContentType", udf(RecommendUtils.getPreviousContentType: String => String), List("pathSub")),
    UserDefinedColumn("recommendSlotIndex", udf(RecommendUtils.getRecommendSlotIndex: String => String), List("pathMain")),
    UserDefinedColumn("searchFrom", udf(SearchUtils.getSearchFrom: (String,String,String) => String),List("pathMain", "path","flag")),
    UserDefinedColumn("searchKeyword", udf(SearchUtils.getSearchKeyword: (String,String,String) => String),List("pathMain", "path", "flag")),
    UserDefinedColumn("searchFromHotWord", udf(SearchUtils.isSearchFromHotWord: String => Int),List("extraPath")),
    UserDefinedColumn("pageEntrancePageCode", udf(PageEntrancePathParseUtils.getPageEntrancePageCode: (String, String,String,String,String) => String), List("pathMain", "path", "pathSub","contentType","flag")),
    UserDefinedColumn("pageEntranceAreaCode", udf(PageEntrancePathParseUtils.getPageEntranceAreaCode: (String, String,String,String,String) => String), List("pathMain", "path", "pathSub","contentType","flag")),
    UserDefinedColumn("pageEntranceLocationCode", udf(PageEntrancePathParseUtils.getPageEntranceLocationCode: (String, String,String,String,String) => String), List("pathMain", "path", "pathSub","contentType","flag")),
    UserDefinedColumn("subject_entrance_page", udf(getSubjectEntrance: (String, String, String, String, String, String) => String), List("pathMain", "path", "flag", "pathSub", "contentType", "pathSpecial")),
    UserDefinedColumn("channel_entrance_page", udf(getChannelEntrance: (String, String, String) => String), List("pathMain", "path", "flag")),
    UserDefinedColumn("sid", udf(getSid _), List("videoSid", "episodeSid"))
  )

  /**
    * step 3, left join dimension table,get sk
    * */
  dimensionColumns = List(
    /** 获得列表页sk source_site_sk */
    ListCategoryUtils.getSourceSiteSK(),

    /** 获得专题 subject_sk */
    SubjectUtils.getSubjectSK(),

    /** 获得筛选sk */
    FilterCategoryUtils.getRetrievalSK(),

    /** 获得推荐来源sk */
    RecommendUtils.getRecommendPositionSK(),

    /** 获得搜索来源sk */
    SearchUtils.getSearchSK(),

    /** 获得频道主页来源维度sk（只有少儿，音乐，体育有频道主页来源维度）*/
    PageEntrancePathParseUtils.getPageEntranceSK(),

    /** 获得首页入口 launcher_entrance_sk */
    EntranceTypeUtils.getLauncherEntranceSK(),


    /** 获得访问ip对应的地域维度user_web_location_sk */
    new DimensionColumn("dim_web_location",
      List(DimensionJoinCondition(Map("ipKey" -> "web_location_key"))),
      "web_location_sk","access_web_location_sk"),

    /** 获得用户维度user_sk */
    new DimensionColumn("dim_medusa_terminal_user",
      List(DimensionJoinCondition(Map("userId" -> "user_id"))),
      List(("user_sk","user_sk"), ("web_location_sk", "user_web_location_sk"))),

    /** 获得用户登录维度user_login_sk */
//    new DimensionColumn("dim_medusa_terminal_user_login",
//      List(DimensionJoinCondition(Map("userId" -> "user_id"))),
//      "user_login_sk"),

    /** 获得设备型号维度product_model_sk */
    new DimensionColumn("dim_medusa_product_model",
      List(DimensionJoinCondition(Map("productModel" -> "product_model"))),
      "product_model_sk"),

   /** 获得推广渠道维度promotion_sk */
    //    new DimensionColumn("dim_medusa_promotion",
    //      List(DimensionJoinCondition(Map("promotionChannel" -> "promotion_code"))),
    //      "promotion_sk"),


    new DimensionColumn("dim_medusa_promotion_channel",
      List(DimensionJoinCondition(Map("promotionChannel" -> "promotion_channel"))),
      "promotion_channel_sk"),



    /** 获得app版本维度app_version_sk */
    new DimensionColumn("dim_app_version",
      List(
        DimensionJoinCondition(Map("apkSeries" -> "app_series", "apkVersion" -> "version", "buildDate" -> "build_time")),
        DimensionJoinCondition(Map("apkSeries" -> "app_series", "apkVersion" -> "version"))
      ),
      "app_version_sk"),

    /** 获得节目维度program_sk */
    new DimensionColumn("dim_medusa_program",
      List(DimensionJoinCondition(Map("sid" -> "sid"))),
      "program_sk"),

    /** 获得上级节目维度previous_program_sk */
    new DimensionColumn("dim_medusa_program","dim_medusa_program_previous",
      List(DimensionJoinCondition(Map("previousSid" -> "sid"))),
      "program_sk","previous_program_sk")

    /** 获得剧集节目维度episode_program_sk*/
    //    new DimensionColumn("dim_medusa_program", "dim_medusa_program_episode",
    //      List(DimensionJoinCondition(Map("episodeSid" -> "sid"))),
    //      "program_sk","episode_program_sk"),

//    /** 获得账号维度account_sk*/
//    new DimensionColumn("dim_medusa_account",
//      List(DimensionJoinCondition(Map("accountId" -> "account_id"))),
//      "account_sk"),
//
//    /** 获得音乐精选集维度mv_topic_sk*/
//    new DimensionColumn("dim_medusa_mv_topic",
//      List(DimensionJoinCondition(Map("omnibusSid" -> "mv_topic_sid"))),
//      "mv_topic_sk"),
//
//    /** 获得歌手维度singer_sk*/
//    new DimensionColumn("dim_medusa_singer",
//      List(DimensionJoinCondition(Map("singerSid" -> "singer_id"))),
//      "singer_sk"),
//
//    /** 获得电台维度mv_radio_sk*/
//    new DimensionColumn("dim_medusa_mv_radio",
//      List(DimensionJoinCondition(Map("station" -> "mv_radio_title"))),
//      "mv_radio_sk")

//    /** 获得音乐榜单维度mv_hot_sk */
//    new DimensionColumn("dim_medusa_mv_hot_list",
//      List(DimensionJoinCondition(Map("topRankSid" -> "mv_hot_rank_id"))),
//      "mv_hot_sk")
  )


  /**
    * step 4,保留哪些列，以及别名声明
    * */

  dimensionsNeedInFact = List("dim_medusa_subject", "dim_medusa_program","dim_medusa_source_site","dim_medusa_page_entrance")

  columnsFromSource = List(
    //作为测试字段,验证维度解析是否正确，上线后删除
 /*   ("subjectName", "subjectName"),
    ("subjectCode", "subjectCode"),
    ("mainCategory", "mainCategory"),
    ("secondCategory", "secondCategory"),
    ("thirdCategory", "thirdCategory"),
    ("fourthCategory", "fourthCategory"),
    ("launcherAreaCode", "launcherAreaCode"),
    ("launcherLocationCode", "launcherLocationCode"),
    ("filterContentType", "filterContentType"),
    ("filterCategoryFirst", "filterCategoryFirst"),
    ("filterCategorySecond", "filterCategorySecond"),
    ("filterCategoryThird", "filterCategoryThird"),
    ("filterCategoryFourth", "filterCategoryFourth"),
    ("recommendSourceType", "recommendSourceType"),
    ("previousSid", "previousSid"),
    ("previousContentType", "previousContentType"),
    ("recommendSlotIndex", "recommendSlotIndex"),
    ("recommendType", "recommendType"),
    ("recommendLogType", "recommendLogType"),
    ("pageEntranceAreaCode", "pageEntranceAreaCode"),
    ("pageEntranceLocationCode", "pageEntranceLocationCode"),
    ("pageEntrancePageCode", "pageEntrancePageCode"),
    ("ipKey", "ipKey"),
    ("account_id", "accountId"),
    ("pathMain", "pathMain"),
    ("path", "path"),
    ("pathSpecial", "pathSpecial"),
    ("pathSub", "pathSub"),
    ("searchFrom", "searchFrom"),
    ("resultIndex", "resultIndex"),
    ("tabName", "tabName"),
    ("searchFromHotWord", "searchFromHotWord"),*/


//--------在fact_medusa_play表中展示的字段---------
    ("duration", "fDuration"),
    ("program_duration", "cast(programDuration as bigint)"),//programDuration
    //("mid_post_duration", ""),//for now,not online filed
    ("user_id", "userId"),
    ("account_id","accountid"),
    ("start_event", "start_event"),
    ("end_event", "end_event"),
    //("start_time", ""),//for now,not online filed
    //("end_time", ""),//for now,not online filed
    ("content_type", "contentType" ),
    ("play_content_type",
      "case when mainCategory in ('interest','hot','member','cantonese') then  mainCategory " +
        "when pathMain like 'home*hotSubject%' then 'interest' " +
        "when dim_medusa_subject.subject_code like 'VIP%' then 'member' " +
        "when dim_medusa_subject.subject_content_type is not null then dim_medusa_subject.subject_content_type " +
        "when dim_medusa_program.content_type is not null then dim_medusa_program.content_type " +
        "when trim(contentType) = '' then null else contentType end"),
    ("subject_entrance","case when subject_entrance_page = 'hotSubject' then '3.0首页短视频' when subject_entrance_page = 'memberArea' then '3.0首页会员看看' when subject_entrance_page = 'recommendation' then '3.0首页今日推荐' when subject_entrance_page = 'fourth_source_site' then concat('3.0',dim_medusa_source_site.second_category,\"*\",dim_medusa_source_site.third_category,\"*\",dim_medusa_source_site.fourth_category) when subject_entrance_page = 'third_source_site' then concat('3.0',dim_medusa_source_site.site_content_type,\"*\",dim_medusa_source_site.second_category,\"*\",dim_medusa_source_site.third_category) when subject_entrance_page = 'second_source_site' then concat('3.0',dim_medusa_source_site.site_content_type,\"*\",dim_medusa_source_site.second_category) when subject_entrance_page = 'channel_home' then concat('3.0',dim_medusa_page_entrance.page_name) else '3.0其他' end"),
    ("channel_entrance","case when channel_entrance_page = 'hotSubject' then '3.0首页短视频' when channel_entrance_page = 'memberArea' then '3.0首页会员看看' when channel_entrance_page = 'recommendation' then '3.0首页今日推荐'   when channel_entrance_page = 'watch_history' then '3.0观看历史'  when channel_entrance_page = 'collect_watch' then '3.0收藏追看'  when channel_entrance_page like '%movie%' then '3.0电影分类入口'  when channel_entrance_page like '%tv%' then '3.0电视剧分类入口'  when channel_entrance_page like '%zongyi%' then '3.0综艺分类入口'  when channel_entrance_page like '%kids%' then '3.0少儿分类入口'  when channel_entrance_page like '%comic%' then '3.0动漫分类入口'  when channel_entrance_page like '%game%' then '3.0游戏电竞分类入口'  when channel_entrance_page like '%hot%' then '3.0资讯分类入口'  when channel_entrance_page like '%interest%' then '3.0奇趣分类入口'  when channel_entrance_page like '%sports%' then '3.0体育分类入口'  when channel_entrance_page like '%member%' then '3.0会员分类入口'  when channel_entrance_page like '%jilu%' then '3.0纪实分类入口'  when channel_entrance_page like '%mv%' then '3.0音乐分类入口'  when channel_entrance_page like '%cantonese%' then '3.0粤语分类入口' when channel_entrance_page like '%xiqu%' then '3.0戏曲分类入口' else '3.0其他' end"),
    ("is_reservation", "case when trim(contentType) = 'reservation' then 'true' else 'false' end"),
    ("search_keyword", "searchKeyword"),
    ("product_model", "productModel"),
//    ("auto_clarity", "tencentAutoClarity"),
    ("contain_ad", "case when containAd = '1' then 'true' else 'false' end"),
    ("app_enter_way", "appEnterWay"),
    ("play_type", "playType"),
    //("session_id", "sessionId"),//for now,not online filed
    //("device_id", "deviceId"),//for now,not online filed
    //("display_id", "displayId"),//for now,not online filed
    //("player_type", "playerType"),//for now,not online filed
    ("version_flag", "flag"),
    ("launcher_recommend_index", "launcherRecommendIndex"),
    ("recommend_type", "recommendType"),
    ("is_group_subject", "isgroupsubject"), //new column at V3.1.8
    ("definition", "definition"), //new column at V3.1.8
    ("definition_source", "definitionsource"), //new column at V3.1.8
    ("path_str", "pathMain"),
    ("dim_date", "dim_date"),
    ("dim_time", "dim_time"),
    ("alg","alg"),
    ("biz","biz"),
    ("realip","realip")
  )

 //will use common util function class instead
  def getIpKey(ip: String): Long = {
    try {
      val ipInfo = ip.split("\\.")
      if (ipInfo.length >= 3) {
        (((ipInfo(0).toLong * 256) + ipInfo(1).toLong) * 256 + ipInfo(2).toLong) * 256
      } else 0
    } catch {
      case ex: Exception => 0
    }
  }

  def getDimDate(dateTime: String): String = {
    try {
      val dateTimeInfo = dateTime.split(" ")
      if (dateTimeInfo.length >= 2) {
        dateTimeInfo(0)
      } else ""
    } catch {
      case ex: Exception => ""
    }
  }

  def getDimTime(dateTime: String): String = {
    try {
      val dateTimeInfo = dateTime.split(" ")
      if (dateTimeInfo.length >= 2) {
        dateTimeInfo(1)
      } else ""
    } catch {
      case ex: Exception => ""
    }
  }

  def getAppSeries(seriesAndVersion: String): String = {
    try {
      val index = seriesAndVersion.lastIndexOf("_")
      if (index > 0) {
        seriesAndVersion.substring(0, index)
      } else ""
    } catch {
      case ex: Exception => ""
    }
  }

  def getAppVersion(seriesAndVersion: String): String = {
    try {
      val index = seriesAndVersion.lastIndexOf("_")
      if (index > 0) {
        seriesAndVersion.substring(index + 1, seriesAndVersion.length)
      } else ""
    } catch {
      case ex: Exception => ""
    }
  }

  def writeToHDFS(df: DataFrame, path: String): Unit = {
    println(s"write df to $path")
    val isBaseOutputPathExist = HdfsUtil.IsDirExist(path)
    if (isBaseOutputPathExist) {
      HdfsUtil.deleteHDFSFileOrPath(path)
      println(s"删除目录: $path")
    }
    //println("记录条数:" + df.count())
    println("输出目录为：" + path)
    println("time：" + Calendar.getInstance().getTime)
    df.write.parquet(path)
  }

  def getSubjectEntrance(pathMain:String,path:String,flag:String,pathSub:String,contentType:String,pathSpecial:String): String ={
    var result: String = null
      if (pathSpecial != null && pathSpecial.contains("subject")) {
        val launcherAreaCode: String =  EntranceTypeUtils.getEntranceAreaCode(pathMain, path, flag)
        if (launcherAreaCode == "hotSubject" || launcherAreaCode == "memberArea" || launcherAreaCode == "recommendation") {
          result = launcherAreaCode
        }

        val mainCategory: String = ListCategoryUtils.getListMainCategory(pathMain, path, flag)
        val secondCategory: String = ListCategoryUtils.getListSecondCategory(pathMain, path, flag)
        val thirdCategory: String = ListCategoryUtils.getListThirdCategory(pathMain, path, flag)
        val fourthCategory: String = ListCategoryUtils.getListFourthCategory(pathMain, path, flag)

        if (result == null && fourthCategory != null) {
          result = "fourth_source_site"
        }

        else if(result == null && fourthCategory == null && thirdCategory != null){
          result = "third_source_site"
        }

        else if(result == null && fourthCategory == null && thirdCategory == null && secondCategory != "site_game" && contentType != "game" && secondCategory != null){
          result = "second_source_site"
        }

        val channelHomePage: String = PageEntrancePathParseUtils.getPageEntrancePageCode(pathMain, path, pathSub, contentType, flag)
        if (result == null && channelHomePage != null) {
          result = "channel_home"
        }
      }

    result
  }

  def getChannelEntrance(pathMain:String,path:String,flag:String): String ={
    var result:String = null
    val launcherAreaCode = EntranceTypeUtils.getEntranceAreaCode(pathMain, path, flag)
    if (launcherAreaCode == "hotSubject" || launcherAreaCode == "memberArea" || launcherAreaCode == "recommendation") {
      result = launcherAreaCode
    }
    else if (pathMain != null && (pathMain.contains("accountcenter_home*观看历史") || pathMain.contains("collect*观看历史"))){
      result = "watch_history"
    }
    else if (pathMain != null && (pathMain.contains("accountcenter_home*收藏追看") || pathMain.contains("collect*收藏追看"))){
      result = "collect_watch"
    }
    else if(pathMain != null && launcherAreaCode == "classification" && pathMain.split("\\*").length >= 2){
      result = pathMain.split("\\*")(2)
    }
    result
  }

  def getSid(videoSid: String, episodeSid: String) = {
    if (episodeSid == null || episodeSid.isEmpty) videoSid
    else episodeSid
  }
}
