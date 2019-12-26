package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.fact.constant.LogPath
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.DataExtractUtils
import org.apache.spark.sql.DataFrame

/**
  * 创建人：郭浩
  * 创建时间：2017/5/19
  * 程序作用：首页各区域、频道首页各区域、站点树、语音搜索等各种来源标识
  * 数据输入：
  * 数据输出：各区域来源维度
  */
object AreaSourceAgg extends DimensionBase {
  columns.skName = "area_source_agg_sk"
  columns.primaryKeys = List("source_code","module_code","sub_module_code")
  columns.trackingColumns = List()
  columns.allColumns = List(
    "source_code",
    "source_name",
    "module_code",
    "module_name",
    "sub_module_code",
    "sub_module_name"
  )

  dimensionName = "dim_whaley_area_source_agg"

  fullUpdate = true

  override def readSource(readSourceType: Value): DataFrame = {
    /**
      * 站点树维度
      */
    val sourceSiteDF = DataExtractUtils.readFromParquet(sqlContext,LogPath.DIM_WHALEY_SOURCE_SITE)
                   .filter("dim_invalid_time is null")
                   .selectExpr("'source_site' as source_code",
                     "'站点树' as source_name",
                     "last_second_code as module_code",
                     "last_second_name as module_name",
                     "last_first_code as sub_module_code",
                     "last_first_name as sub_module_name"
                   )
                     .dropDuplicates(Array("module_code","sub_module_code"))
    /**
      * 首页入口维度
      */
    val distinctFields=Array("module_code","sub_module_code")
    val launcherEntranceDF = DataExtractUtils.readFromParquet(sqlContext,LogPath.DIM_WHALEY_LAUNCHER_ENTRANCE)
      .filter("dim_invalid_time is null")
      .selectExpr("'launcher_entrance' as source_code",
        "'首页入口各区域' as source_name",
        "access_area_code as module_code",
        "access_area_name as module_name",
        "access_location_code as sub_module_code",
        "access_location_name as sub_module_name")
      .orderBy("module_code","sub_module_code").dropDuplicates(distinctFields)

    /**
      * 各频道首页维度
      */
    val channelEntranceDF = DataExtractUtils.readFromParquet(sqlContext,LogPath.DIM_WHALEY_PAGE_ENTRANCE)
      .filter("dim_invalid_time is null")
      .selectExpr("'channel_entrance' as source_code",
        "'频道首页各区域' as source_name",
        "page_code as module_code",
        "page_name as module_name",
        "area_code as sub_module_code",
        "area_name as sub_module_name")
      .dropDuplicates(Array("module_code","sub_module_code"))

    val sc1 = sqlContext
    import sc1.implicits._
//    case class VoiceSearch(source_code:String,source_name:String,module_code:String,module_name:String,sub_module_code:String,sub_module_name:String)
    /**
      * 语音
      */
    val arr = Array("voice_search-语音搜索-voice_search-语音搜索-voice_search-语音搜索")
    val voiceSearchDF  = sc.parallelize(arr).map(f=>{
      val source_code = f.split("-")(0)
      val source_name = f.split("-")(1)
      val module_code = f.split("-")(2)
      val module_name = f.split("-")(3)
      val sub_module_code = f.split("-")(4)
      val sub_module_name = f.split("-")(5)
      (source_code,source_name,module_code,module_name,sub_module_code,sub_module_name)
    }).toDF("source_code","source_name","module_code","module_name","sub_module_code","sub_module_name")


    sourceSiteDF.unionAll(launcherEntranceDF).unionAll(channelEntranceDF).unionAll(voiceSearchDF)

  }
}