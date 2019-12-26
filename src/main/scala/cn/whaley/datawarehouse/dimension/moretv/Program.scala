package cn.whaley.datawarehouse.dimension.moretv

import java.sql.Timestamp

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType
import cn.whaley.datawarehouse.util.{DataExtractUtils, MysqlDB}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
  * Created by Tony on 17/3/10.
  *
  * 电视猫节目维度表
  */
object Program extends DimensionBase {

  columns.skName = "program_sk"
  columns.primaryKeys = List("sid")
  columns.trackingColumns = List()
  columns.allColumns = List("sid", "title", "content_type", "content_type_name", "duration", "video_type", "episode_index",
    "status", "type", "parent_sid", "parent_title", "area", "year", "video_length_type", "create_time", "publish_time",
    "supply_type", "package_code", "package_name", "is_vip", "tid", "qq_id","video_sources","copyright_code","virtual_sid","virtual_title",
  "director","actor","tags")

  val vipProgramColumns = List("program_code","package_code","package_name")

  //  sourceDb = MysqlDB.medusaCms("mtv_basecontent", "id", 1, 2010000000, 500)
  //  sourceDb = MysqlDB.medusaProgramInfo("mtv_program", "id", 1, 2010000000, 2000)
  val contentProgramDB = MysqlDB.medusaMemberDB("content_program")
  val contentPackageProgramDB = MysqlDB.medusaMemberDB("content_package_program_relation")

  val virtualDb = MysqlDB.medusaProgramInfo("mtv_virtual_content_rel","id",1,2010000000,500)

  val smallVideoDb = MysqlDB.medusaSmallVideoProgramInfo("tag_lmv_program","id",1,2010000000,500)


  dimensionName = "dim_medusa_program"

  sourceTimeCol = "publish_time"

  fullUpdate = true

  override def readSource(readSourceType: SourceType.Value): DataFrame = {
    DataExtractUtils.readFromOds(sqlContext, "ods_view.db_snapshot_mysql_medusa_mtv_program", "latest", "latest")
  }

  override def filterSource(sourceDf: DataFrame): DataFrame = {
    //    sourceDf.persist()
    val s = sqlContext
    import s.implicits._
    sqlContext.read.format("jdbc").options(virtualDb).load().createOrReplaceTempView("virtual_content_rel")
    val contentProgram = sqlContext.read.format("jdbc").options(contentProgramDB).load()
    val tvbBBCPackageProgram = sqlContext.read.format("jdbc").options(contentPackageProgramDB).load().filter("relation_status = 'bound'")
      .persist(StorageLevel.MEMORY_AND_DISK)
    val tvbPackageProgramDF = tvbBBCPackageProgram.filter("package_code = 'TVB'").select(vipProgramColumns.map(col):_*)
    val bbcPackageProgramDF = tvbBBCPackageProgram.filter("package_code = 'BBC'").select(vipProgramColumns.map(col):_*)
    tvbBBCPackageProgram.unpersist()

    val tencentPackageProgram = contentProgram.filter("source = 'tencent2'")
    val tencentProgramDF = tencentPackageProgram.select("program_code").
      withColumn("package_code",lit("TENCENT")).
      withColumn("package_name", lit("腾讯节目包")).
      select(vipProgramColumns.map(col):_*)
    val vipProgramDF = tencentProgramDF.union(tvbPackageProgramDF).union(bbcPackageProgramDF)
    var newDf = sourceDf.join(vipProgramDF, sourceDf("sid") === vipProgramDF("program_code"), "left_outer").
      withColumn("is_vip", if(col("package_code") != null) lit(1) else lit(0))

    //媒体文件表，包含腾讯tid字段
    //    val mediaFileDb = MysqlDB.medusaCms("mtv_media_file", "id", 1, 250000000, 500)
    val mediaFileDb = MysqlDB.medusaProgramInfo("mtv_media_file")
    val mediaFileDf = sqlContext.read.format("jdbc").options(mediaFileDb).load()
      .where("status = 1 and source = 'tencent2' ")
    //    val mediaFileSourceDf = sqlContext.read.format("jdbc").options(mediaFileDb).load()
    //      .where("status = 1").select("content_Id","source").rdd.
    //      map(row => (row.getInt(0),row.getString(1))).groupByKey().map(x => (x._1,x._2.toList.mkString(","))).toDF("content_Id","video_sources")
    val mediaFileSourceDf = sqlContext.read.format("jdbc").options(mediaFileDb).load()
      .where("status = 1").select("content_Id","source").groupBy("content_Id").agg(expr("concat_ws(',',collect_set(source)) as video_sources"))
    newDf = newDf.join(mediaFileDf, newDf("id") === mediaFileDf("content_Id"), "leftouter")
      .select(newDf("*"), mediaFileDf("video_id").as("tid"))

    newDf = newDf.join(mediaFileSourceDf, newDf("id") === mediaFileSourceDf("content_Id"), "leftouter")
      .select(newDf("*"), mediaFileSourceDf("video_sources"))

    sqlContext.udf.register("myReplace",myReplace _)

    newDf.createOrReplaceTempView("mtv_program")

    val programDf = sqlContext.sql("select sid, id, title, contentType, " +
      " duration, parentId,videoType, type, " +
      " status, " +
      " episode, area, year, " +
      "director, actor, tags," +
      " videoLengthType, createTime as create_time, publishTime as publish_time, supply_type ,package_code, package_name, is_vip, tid, qqId as qq_id,video_sources,copyright_code " +
      " from mtv_program where sid is not null and sid <> '' and title is not null ")

    programDf.createOrReplaceTempView("program_table_tmp")

    /**
      * 此处修正的是两条记录具有相同的sid，但是一个是失效的，将失效的过滤掉
      */
    sqlContext.sql(
      """
        |select sid, id, title, contentType,duration, parentId,videoType, type,status,episode, area, year,
        |videoLengthType,create_time,publish_time,supply_type ,package_code, package_name, is_vip, tid,qq_id
        |,video_sources,copyright_code,first_value(status) over (partition by sid order by status desc) as status_flag,
        |director, actor, tags
        |from program_table_tmp
      """.stripMargin).filter("status_flag = status").createOrReplaceTempView("program_table")

    sqlContext.cacheTable("program_table")

    sqlContext.sql("select id,sid,title,contentType from program_table where videoType = '1'")
      .createOrReplaceTempView("program_head_table") //剧头信息表

    val contentTypeDb = MysqlDB.medusaCms("mtv_content_type", "id", 1, 100, 1)

    sqlContext.read.format("jdbc").options(contentTypeDb).load().createOrReplaceTempView("content_type")

    sqlContext.sql("SELECT a.sid, if(a.videoType = '2',b.sid,a.sid) as parent_sid, " +
      "if(a.videoType = '2',myReplace(b.title),myReplace(a.title)) as parent_title, " +
      "myReplace(a.title) as title, a.status, a.type," +
      "if(a.videoType = '2',b.contentType,a.contentType) as content_type, c.name as content_type_name, " +
      "a.duration, a.videoType as video_type, a.episode as episode_index, a.area, a.year, a.videoLengthType as video_length_type" +
      ",a.supply_type, a.package_code, a.package_name, a.is_vip, a.tid, a.qq_id, a.create_time, a.publish_time,a.video_sources,a.copyright_code, " +
      "a.director, a.actor, a.tags" +
      " from program_table a" +
      " left join program_head_table b on a.parentId = b.id" +
      " left join content_type c on if(myReplace(a.videoType)= '2',b.contentType,a.contentType) = c.code " +
      " where a.sid is not null and a.sid <> ''" +
      " ORDER BY a.id").createOrReplaceTempView("real_program")
    val df = sqlContext.sql(
      """
        |select a.sid,a.parent_sid,a.parent_title,a.title,a.status,a.type,a.content_type,a.content_type_name,a.duration,a.video_type,a.episode_index,a.area,a.year,a.video_length_type,a.supply_type,a.package_code,a.package_name,a.is_vip,a.tid,a.qq_id,a.create_time,a.publish_time,a.video_sources,a.copyright_code,if(b.virtual_sid is not null,b.virtual_sid,a.parent_sid) as virtual_sid,if(b.title is not null,b.title,a.parent_title) as virtual_title,a.director, a.actor, a.tags
        |from real_program a
        |left join virtual_content_rel b
        |on a.parent_sid=b.sid
      """.stripMargin).dropDuplicates(List("sid"))
    val smallVideoDF = sqlContext.read.format("jdbc").options(smallVideoDb).load()
      .selectExpr("sid",
        "sid as parent_sid",
        "title as parent_title",
        "title",
        "status",
        "cast(null as integer) as type",
        "type as content_type",
        "'小视频' as content_type_name",
        "duration",
        "cast(null as integer) as video_type",
        "cast(null as integer) as episode_index",
        "cast(null as string) as area",
        "cast(null as string) as year",
        "cast(null as integer) as video_length_type",
        "cast(null as string) as supply_type",
        "cast(null as string) as package_code",
        "cast(null as string) as package_name",
        "cast(null as integer) as is_vip",
        "cast(null as string) as tid",
        "cast(null as string) as qq_id",
        "create_datetime as create_time",
        "cast(null as timestamp) as publish_time",
        "source as video_sources",
        "cast(null as string) as copyright_code",
        "sid as virtual_sid",
        "title as virtual_title",
        "null as director",
        "null as actor",
        "null as tags"
      )

    //过滤掉失效记录
    val unionDF = df.union(smallVideoDF).filter("status = 1")
    unionDF
  }

  def myReplace(s:String): String ={
    if (s != null)
      s
        .replace("'", "")
        .replace("\t", " ")
        .replace("\r\n", "-")
        .replace("\r", "-")
        .replace("\n", "-")
    else null
  }

}
