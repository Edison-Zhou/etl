package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


/**
  * Created by lituo on 17/9/26.
  *
  * 页面维度
  *
  * 包含首页、频道页和其他各种页面
  */
object Page extends DimensionBase {

  dimensionName = "dim_medusa_page"

  columns.skName = "page_sk"
  columns.primaryKeys = List("page_id")
  columns.trackingColumns = List()
  columns.allColumns = List(
    "page_id",
    "page_code",
    "page_name",
    "area_code",
    "area_name",
    "location_code",
    "location_name",
    "content_type")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "page_id" -> "id"
  )

  fullUpdate = true

  override def readSource(readSourceType: Value): DataFrame = {

    //首页
    val launcherSourceDb = MysqlDB.dwDimensionDb("moretv_launcher_entrance")
    val launcherDfOrigin = sqlContext.read.format("jdbc").options(launcherSourceDb).load()
    val launcherDf = launcherDfOrigin.withColumn("page_code", lit("launcher"))
      .withColumn("page_name", lit("首页")).withColumn("content_type", lit("home"))
      .selectExpr("id", "page_code", "page_name", "area_code", "area_name", "location_code", "location_name", "content_type")

    //自定义页面
    val pageSourceDb = MysqlDB.dwDimensionDb("moretv_page_entrance")
    val pageDfOrigin = sqlContext.read.format("jdbc").options(pageSourceDb).load()
    val pageDf = pageDfOrigin.withColumn("id", expr("id + 100000"))
      .selectExpr("id", "page_code", "page_name", "area_code", "area_name", "location_code", "location_name", "content_type")


    //列表页
    val siteSourceDb = MysqlDB.medusaCms("mtv_program_site", "id", 1, 20000, 10)
    val siteDfOrigin = sqlContext.read.format("jdbc").options(siteSourceDb).load()
    siteDfOrigin.persist()

    val siteDf =
    //电影列表页
      siteDfOrigin.where(expr("parentId = 8711 and status = 1")).withColumn("page_code", lit("list_movie")).
        withColumn("page_name", lit("电影列表页")).withColumn("content_type", lit("movie")
      ).unionAll(
        //电视剧列表页
        siteDfOrigin.where(expr("parentId = 8471 and status = 1")).withColumn("page_code", lit("list_tv")).
          withColumn("page_name", lit("电视剧列表页")).withColumn("content_type", lit("tv"))
      ).unionAll(
        //综艺列表页
        siteDfOrigin.where(expr("parentId = 9651 and status = 1")).withColumn("page_code", lit("list_zongyi")).
          withColumn("page_name", lit("综艺列表页")).withColumn("content_type", lit("zongyi"))
      ).unionAll(
        //纪录片列表页
        siteDfOrigin.where(expr("parentId = 9431 and status = 1")).withColumn("page_code", lit("list_jilu")).
          withColumn("page_name", lit("纪录片列表页")).withColumn("content_type", lit("jilu"))
      ).unionAll(
        //动漫列表页
        siteDfOrigin.where(expr("parentId = 9901 and status = 1")).withColumn("page_code", lit("list_comic")).
          withColumn("page_name", lit("动漫列表页")).withColumn("content_type", lit("comic"))
      ).unionAll(
        //戏曲列表页
        siteDfOrigin.where(expr("parentId = 8841 and status = 1")).withColumn("page_code", lit("list_xiqu")).
          withColumn("page_name", lit("戏曲列表页")).withColumn("content_type", lit("xiqu"))
      ).unionAll(
        //奇趣列表页
        siteDfOrigin.where(expr("parentId = 11168 and status = 1")).withColumn("page_code", lit("list_interest")).
          withColumn("page_name", lit("奇趣列表页")).withColumn("content_type", lit("interest"))
      ).unionAll(
        //资讯列表页
        siteDfOrigin.where(expr("parentId in (11185, 9331) and status = 1")).withColumn("page_code", lit("list_hot")).
          withColumn("page_name", lit("资讯列表页")).withColumn("content_type", lit("hot"))
      ).unionAll(
        //直播列表页
        siteDfOrigin.where(expr("parentId = 11076")).withColumn("page_code", lit("list_webcast")).
          withColumn("page_name", lit("直播列表页")).withColumn("content_type", lit("webcast"))
      ).unionAll(
        //粤语列表页
        siteDfOrigin.where(expr("parentId = 11613")).withColumn("page_code", lit("list_cantonese")).
          withColumn("page_name", lit("粤语列表页")).withColumn("content_type", lit("cantonese"))
      ).unionAll(
        //会员列表页
        siteDfOrigin.where(expr("parentId = 11627")).withColumn("page_code", lit("list_member")).
          withColumn("page_name", lit("会员列表页")).withColumn("content_type", lit("member"))
        //      ).unionAll(
        //        //听儿歌列表页
        //        siteDfOrigin.where(expr("parentId = 10261 and status = 1")).withColumn("page_code", lit("list_tingerge")).
        //          withColumn("page_name", lit("听儿歌列表页")).withColumn("content_type", lit("kids"))
        //      ).unionAll(
        //        //看动画列表页
        //        siteDfOrigin.where(expr("parentId = 10351 and status = 1")).withColumn("page_code", lit("list_kandonghua")).
        //          withColumn("page_name", lit("看动画列表页")).withColumn("content_type", lit("kids"))
        //      ).unionAll(
        //        //学知识列表页
        //        siteDfOrigin.where(expr("parentId = 10171 and status = 1")).withColumn("page_code", lit("list_xuezhishi")).
        //          withColumn("page_name", lit("学知识列表页")).withColumn("content_type", lit("kids"))
      ).selectExpr("id", "page_code", "page_name",
        "code as area_code", "name as area_name", "null as location_code", "null as location_name", "content_type")
        .dropDuplicates(List("page_code", "area_code", "location_code"))
        .withColumn("id", expr("id + 1000000"))


    val siteSecondaryOriginDf = siteDfOrigin.as("a").join(siteDfOrigin.as("b"), expr("a.parentId = b.id"))
      .selectExpr("a.*", "b.parentId as grandParentId", "b.code as parentCode", "b.name as parentName", "b.status as parentStatus")

    val siteSecondaryDf =
    //音乐的二级列表页
      siteSecondaryOriginDf.where(expr("grandParentId = 450 and status = 1 and parentStatus = 1"))
        .withColumn("page_code", expr("concat('list_', parentCode)"))
        .withColumn("page_name", expr("concat(parentName, '列表页')")).withColumn("content_type", lit("mv"))
        .unionAll(
          //听儿歌列表页
          siteSecondaryOriginDf.where(expr("grandParentId = 10161 and status = 1 and parentStatus = 1"))
            .withColumn("page_code", expr("concat('list_', parentCode)"))
            .withColumn("page_name", expr("concat(parentName, '列表页')")).withColumn("content_type", lit("kids"))
        ).selectExpr("id", "page_code", "page_name",
        "code as area_code", "name as area_name", "null as location_code", "null as location_name", "content_type")
        .dropDuplicates(List("page_code", "area_code", "location_code"))
        .withColumn("id", expr("id + 1000000"))

    launcherDf.unionAll(pageDf).unionAll(siteDf).unionAll(siteSecondaryDf)


  }


}
