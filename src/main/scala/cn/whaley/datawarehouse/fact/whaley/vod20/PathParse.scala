package cn.whaley.datawarehouse.fact.whaley.vod20

import java.io.File
import java.util

import cn.whaley.bigdata.dw.path.PathParser
import cn.whaley.bigdata.wdata.common.WDSdk
import cn.whaley.bigdata.wdata.common.agent.HbaseAgent
import cn.whaley.bigdata.wdata.common.global.CompareOperation
import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.fact.constant.Constants._
import cn.whaley.datawarehouse.util.{DataExtractUtils, DateFormatUtils, HdfsUtil, Params}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import scala.collection.JavaConversions._


/**
  * Created by lituo on 2018/2/28.
  */
object PathParse extends BaseClass {

  implicit val formats = Serialization.formats(NoTypeHints)

  private val OUTPUT_PATH = FACT_HDFS_BASE_PATH_TMP + File.separator + "whaley_path_parse" + File.separator  //中间结果目录

  /**
    * 源数据读取函数, ETL中的Extract
    * 如需自定义，可以在子类中重载实现
    *
    * @return
    */
  override def extract(params: Params): DataFrame = {
    val date = params.startDate
    val hour = params.startHour
    //基于上个小时的快照，生成这个小时使用的hbase表
    val hbaseAgent = WDSdk.getHbaseAgent
    val namespaceAsString = "dw"
    val tableName = s"path_parser_info_$date$hour"
    val (date_1,hour_1) = DateFormatUtils.add(date,hour,-1,"hour")
    val snapshotName = s"snapshot_path_parser_info_$date_1$hour_1"

    //如果数据表已存在则先进行删除
    if(params.isRefresh){
      hbaseAgent.delete("table",s"$namespaceAsString:$tableName")
      hbaseAgent.delete("snapshot",s"snapshot_path_parser_info_$date$hour")
    }
    /*执行时间过长，先注释掉
    //如果快照不存在则直接报错
    if(!hbaseAgent.snapshotExists(snapshotName)){
      throw new RuntimeException(s"Hbase快照[$snapshotName]不存在，请检查上一小时是否未执行成功！")
    }*/
    hbaseAgent.produceTableBySnapshot(namespaceAsString, tableName, snapshotName)
    //清除掉过期的rowkey
    val (date_4,hour_4) = DateFormatUtils.add(date,hour,-4,"hour")
    val dateCN_4 = DateFormatUtils.toDateCN(date_4)
    val list: util.List[String] = hbaseAgent.queryBySingleColumnValueFilter(namespaceAsString, tableName, "meta:final_datetime", CompareOperation.LESS_OR_EQUAL, s"$dateCN_4 $hour_4:00:00")

    for (rowKey <- list) {
      hbaseAgent.deleteOneRecord(namespaceAsString,tableName,rowKey)
    }
    readSource(date, hour)
  }

  def readSource(sourceDate: String, sourceHour: String) = {
    val spark2 = spark
    import spark2.implicits._

    val pathTables = Seq(
      "log_whaleytv_main_path_channelhome_click",
      "log_whaleytv_main_path_columncenter_click",
      "log_whaleytv_main_path_detail",
      "log_whaleytv_main_path_detail_click",
      "log_whaleytv_main_path_episodelist_click",
      "log_whaleytv_main_path_filter",
      "log_whaleytv_main_path_function_sitetree_click",
      "log_whaleytv_main_path_launcher_click",
      "log_whaleytv_main_path_launcher_view",
      "log_whaleytv_main_path_live",
      //      "log_whaleytv_main_path_message_click",
      "log_whaleytv_main_path_play",
      "log_whaleytv_main_path_playexit_click",
      "log_whaleytv_main_path_poster_click",
      "log_whaleytv_main_path_recenttask",
      "log_whaleytv_main_path_searchresult",
      "log_whaleytv_main_path_sitetree_click",
      "log_whaleytv_main_path_star_action",
      "log_whaleytv_main_path_subject_click",
      "log_whaleytv_main_path_tag_click",
      "log_whaleytv_main_path_vip_dailyrecommend_click",
      "log_whaleytv_main_path_voice_searchresult_click",
      "log_whaleytv_main_path_voice_searchresult_return",
      "log_whaleytv_main_path_voiceuse"
      //      "log_whaleytv_main_path_whaleylive"

    )

    var logRdd: RDD[((String, String), Seq[Map[String, Any]])] = null

    pathTables.foreach(odsTableName => {
      val df = DataExtractUtils.readFromOds(sqlContext, "ods_view." + odsTableName, sourceDate, sourceHour)
      val rowSchema = df.schema
      if (rowSchema.fieldNames.contains("logSessionId")
        && rowSchema.fieldNames.contains("logCount")
        && rowSchema.fieldNames.contains("userId")) {
        val rdd = df.rdd.repartition(50).map(r => {
          val session = r.getAs[String]("logSessionId")
          val userId = r.getAs[String]("userId")
          val valueMap = r.getValuesMap[Any](rowSchema.fieldNames)
          //val jsonString = JSON.toJSONString(JavaConversions.mapAsJavaMap(valueMap), new Array[SerializeFilter](0))
          ((userId, session), Seq(valueMap))
        })
        if (logRdd == null) {
          logRdd = rdd
        } else {
          logRdd = logRdd.union(rdd)
        }
      } else {
        println(odsTableName + "不存在logSessionId或者logCount或者UserId字段")
        df.printSchema()
      }
    })


    //val groupDf = dataset.groupByKey(_._1)
    //.reduceGroups((a, b) => (a._1, a._2.++(b._2)))
    //.map(s => s._2)
    logRdd.reduceByKey((x, y) => x.++(y)) //按照session聚合
      .map(rowMap => {
      val rowSeq = rowMap._2.sortBy(log => log.get("logCount") match { // 按logCount排序
        case Some(x: Int) => x
        case Some(x: String) => try {
          x.toInt
        } catch {
          case _:NumberFormatException => Int.MinValue
        }
        case _ => Int.MinValue
      })
      val logSessionId = rowMap._1._2
      val userId = rowMap._1._1
      //          val userId = if (rowSeq.nonEmpty) {rowSeq.head("userId")}
      (userId, logSessionId, rowSeq.map(r => write(r))) //序列化成json字符串
    }).toDF()

  }

  /**
    * 数据转换函数，ETL中的Transform
    *
    * @return
    */
  override def transform(params: Params, df: DataFrame): DataFrame = {
    val spark2 = spark
    import spark2.implicits._
    val date = params.startDate
    val hour = params.startHour
    if (debug) {
      val path = OUTPUT_PATH + "log" + File.separator + date + File.separator + hour
      HdfsUtil.deleteHDFSFileOrPath(path)
      df.write.parquet(path)
    }
    val ds = df.map(r => (r.getString(0), r.getString(1), r.getAs[Seq[String]](2)))
    //    ds.map(s => (s._1, write(PathParser.parse(s._2)))).toDF()
    ds.flatMap(s => PathParser.parse(s._1, s._2, date + hour, s._3)) //调用解析方法
      .map(s => (s.logSessionId, s.des, write(s)))
      .toDF("session", "type", "destination")
  }

  /**
    * 数据存储函数，ETL中的Load
    */
  override def load(params: Params, df: DataFrame): Unit = {
    val date = params.startDate
    val hour = params.startHour
    df.persist()
    val desType = List("path_detail", "path_play", "path_live")
    desType.foreach(t => {
      val path = OUTPUT_PATH + t + File.separator + date + File.separator + hour
      HdfsUtil.deleteHDFSFileOrPath(path)
      df.where(s"type='$t'").write.parquet(path)
    })

    //生成hbase快照
    val hbaseAgent: HbaseAgent = WDSdk.getHbaseAgent
    val namespaceAsString = "dw"
    val tableName = s"path_parser_info_$date$hour"
    val snapshotName: String = "snapshot_" + tableName
    hbaseAgent.snapshot(namespaceAsString, tableName, snapshotName)


  }


}
