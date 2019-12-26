package cn.whaley.datawarehouse.normalized.medusa

import cn.whaley.datawarehouse.normalized.NormalizedEtlBase
import cn.whaley.datawarehouse.util.{MysqlDB, Params}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.storage.StorageLevel

/**
  * Created by lituo on 2017/12/19.
  */
object Program extends NormalizedEtlBase {
  /**
    * 源数据读取函数, ETL中的Extract
    * 如需自定义，可以在子类中重载实现
    *
    * @return
    */

  tableName = "medusa_program_base_info"

  val vipProgramColumns = List("program_code", "package_code", "package_name")

  override def extract(params: Params) = {
    val sourceDb = MysqlDB.medusaCms("mtv_basecontent", "id", 1, Int.MaxValue, 500)
    val contentProgramDB = MysqlDB.medusaMemberDB("content_program")
    val contentPackageProgramDB = MysqlDB.medusaMemberDB("content_package_program_relation")

    val sourceDf = sqlContext.read.format("jdbc").options(sourceDb).load()

    val contentProgram = sqlContext.read.format("jdbc").options(contentProgramDB).load()
    val tvbBBCPackageProgram = sqlContext.read.format("jdbc").options(contentPackageProgramDB).load().filter("relation_status = 'bound'")
      .persist(StorageLevel.MEMORY_AND_DISK)
    val tvbPackageProgramDF = tvbBBCPackageProgram.filter("package_code = 'TVB'").select(vipProgramColumns.map(col): _*)
    val bbcPackageProgramDF = tvbBBCPackageProgram.filter("package_code = 'BBC'").select(vipProgramColumns.map(col): _*)
    tvbBBCPackageProgram.unpersist()

    val tencentPackageProgram = contentProgram.filter("source = 'tencent2'")
    val tencentProgramDF = tencentPackageProgram.select("program_code").
      withColumn("package_code", lit("TENCENT")).
      withColumn("package_name", lit("腾讯节目包")).
      select(vipProgramColumns.map(col): _*)
    val vipProgramDF = tencentProgramDF.union(tvbPackageProgramDF).union(bbcPackageProgramDF)
    var newDf = sourceDf.join(vipProgramDF, sourceDf("sid") === vipProgramDF("program_code"), "left_outer").
      withColumn("is_vip", if (col("package_code") != null) lit(1) else lit(0))

    //媒体文件表，包含腾讯tid字段
    val mediaFileDb = MysqlDB.medusaCms("mtv_media_file", "id", 1, 250000000, 500)
    val mediaFileDf = sqlContext.read.format("jdbc").options(mediaFileDb).load()
      .where("status = 1 and source = 'tencent2' ")

    newDf = newDf.join(mediaFileDf, newDf("id") === mediaFileDf("content_id"), "leftouter")
      .select(newDf("*"), mediaFileDf("video_id").as("tid"))
    newDf
  }

  /**
    * 数据转换函数，ETL中的Transform
    *
    * @return
    */
  override def transform(params: Params, df: DataFrame) = {
    sqlContext.udf.register("myReplace", myReplace _)

    df.registerTempTable("mtv_basecontent")

    val programDf = sqlContext.sql("select sid, id, display_name, content_type, " +
      " duration, parent_id, video_type, type, " +
      "(case when status = 1 and origin_status = 1 then 1 else 0 end) status, " +
      " episode, area, year, " +
      " videoLengthType, create_time, publish_time, supply_type ,package_code, package_name, is_vip, tid, qq_id" +
      " from mtv_basecontent where sid is not null and sid <> '' and display_name is not null ")

    programDf.persist()
    programDf.registerTempTable("program_table")

    val contentTypeDb = MysqlDB.medusaCms("mtv_content_type", "id", 1, 100, 1)

    sqlContext.read.format("jdbc").options(contentTypeDb).load().registerTempTable("content_type")

    sqlContext.sql("SELECT a.sid, b.sid as parent_sid, myReplace(a.display_name) as title, a.status, a.type, " +
      "a.content_type, c.name as content_type_name, a.duration, a.video_type, a.episode as episode_index, " +
      "a.area, a.year, a.videoLengthType as video_length_type,a.supply_type, a.package_code, a.package_name, a.is_vip, a.tid, a.qq_id, " +
      "a.create_time, " +
      "a.publish_time " +
      " from program_table a" +
      " left join program_table b on a.parent_id = b.id" +
      " left join content_type c on a.content_type = c.code " +
      " where a.sid is not null and a.sid <> ''" +
      " ORDER BY a.id").dropDuplicates(List("sid"))
  }

  def myReplace(s: String): String = {
    var t = s
    t = t.replace("'", "")
    t = t.replace("\t", " ")
    t = t.replace("\r\n", "-")
    t = t.replace("\r", "-")
    t = t.replace("\n", "-")
    t
  }
}
