package cn.whaley.datawarehouse.fact.moretv.util

import cn.whaley.datawarehouse.global.LogConfig
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by michael on 2017/8/16.
  *
  * RDD方式做电视猫2.x的过滤
  *
  * 整体思路：剔除单个用户在短时间内连续上抛同一个视频的播放日志的信息
  *具体方案：
  * 获取同一个用户播放同一个视频（episodeSid）的有序播放时间戳信息列表A，
  * 对A中的时间信息根据时间间隔阈值x分割获得多组时间段，
  * 计算每段时间内的上抛日志量n以及两条日志之间的平均时间间隔t。
  * 过滤参数值：x=30分钟；n=5；t=5分钟
  *举例：
  *  A A1 A2 B C，每个字母代表一条日志，如果B和C日志之间的时间间隔大于30分钟，那么校验A A1 A2 B这一段数据。
  */
object Play2xFilterUtilsNew extends LogConfig {
  val fact_table_name = "log_data_play2x"
  val filterTable = "filterTable"
  val totalCountFilterTable = "totalCountFilterTable"

  //多组时间段间隔阀值：30分钟
  val time_quantum_threshold = 1800
  //同一个用户播放同一个剧集的播放次数阀值
  val play_times_threshold = 5
  //两条日志之间的平均时间间隔阀值：5分钟
  val avg_second_threshold = 300
  val repartitionNum=50

  /** 获得过滤结果 */
  def get2xFilterDataFrame(factDataFrame: DataFrame, sqlContext: SQLContext): DataFrame = {
    factDataFrame.repartition(repartitionNum).registerTempTable(fact_table_name)
    //println("factDataFrame.count():" + factDataFrame.count())
    val totalFilterSql=
      s"""select  concat_ws('_',userId,episodeSid) as key,count(1) as total
          |from $fact_table_name
          |group by concat_ws('_',userId,episodeSid)
          |having total>1000
       """.stripMargin
     sqlContext.sql(totalFilterSql).registerTempTable(totalCountFilterTable)

    val sqlStr =
      s"""select concat_ws('_',a.userId,a.episodeSid) as key,a.datetime,unix_timestamp(a.datetime) as timestampValue
          |from $fact_table_name a
          |left join  $totalCountFilterTable b on
          |concat_ws('_',a.userId,a.episodeSid)=b.key
          |where b.key is null
       """.stripMargin
    val df = sqlContext.sql(sqlStr)

    import scala.util.control.Breaks._
    val groupByRdd = df.rdd.map(row => (row.getString(0), (row.getString(1), row.getLong(2)))).groupByKey()
    val resultRdd = groupByRdd.map(x => {
      //key is concat_ws('_',userId,episodeSid)
      val key = x._1
      //datetime,timestampValue的tuple的List
      val tupleIterable = x._2
      //按照timestampValue升序排序
      val timeList = tupleIterable.toList.sortBy(_._2)
      val length = timeList.length
      val arrayBuffer = ArrayBuffer.empty[Row]
      var i = 0
      while (i < length) {
        breakable {
          var j = i + 1
          while (j < length - 1) {
            val jTimestampValue=timeList(j)._2
            //取jRow的下一个值
            val k = j + 1
            val kTimestampValue =timeList(k)._2

            //通过两条记录的时间间隔划分出记录区间
            if (kTimestampValue - jTimestampValue > time_quantum_threshold && (k-i>play_times_threshold)) {
              checkAndSaveRecordToArray(key,timeList, i, k, arrayBuffer)
              i = k
              break
            }

            //通过达到记录边界划分记录区间.时间间隔都没有达到检测值time_quantum_threshold，检测全量记录
            if(k==length-1){
              checkAndSaveRecordToArray(key,timeList, i, length, arrayBuffer)
              i = length
              break
            }
            j = j + 1
          }
          i = i + 1
        }
      }
      arrayBuffer.toList
    }).flatMap(x => x)

//    println("df.schema.fields:" + df.schema.fields.foreach(e => println(e.name)))
    val filterDF = sqlContext.createDataFrame(resultRdd, StructType(df.schema.fields))
    filterDF.registerTempTable(filterTable)

    //writeToHDFS(filterDF, baseOutputPathFilter)
    val resultDF = sqlContext.sql(
      s"""select a.*,'startplay' as start_event,'unKnown' as end_event
        | from       $fact_table_name   a
        | left join  $filterTable        b on
        |    concat_ws('_',a.userId,a.episodeSid)=b.key  and
        |    a.datetime=b.datetime
        | left join $totalCountFilterTable c on concat_ws('_',a.userId,a.episodeSid)=c.key
        |where b.key is null and c.key is null
      """.stripMargin).withColumnRenamed("duration","fDuration").withColumnRenamed("datetime","fDatetime")
    resultDF.repartition(repartitionNum)
  }

  //将timeList[左闭右开区间]的数值存入arrayBuffer,用来做过滤,arrayBuffer存储的是黑名单记录
  private def checkAndSaveRecordToArray(key:String,timeList: List[(String,Long)], i: Int, j: Int, arrayBuffer: ArrayBuffer[Row]) {
    val check_play_times_threshold = j - i
    val endRow = timeList.apply(j - 1)
    val endTimestampValue = endRow._2
    val startRow = timeList.apply(i)
    val startTimestampValue = startRow._2
    val check_avg_second_threshold = (endTimestampValue - startTimestampValue) / check_play_times_threshold
    if (check_play_times_threshold > play_times_threshold && check_avg_second_threshold < avg_second_threshold) {
      for (h <- i until j) {
        arrayBuffer.+=(Row(key,timeList.apply(h)._1,timeList.apply(h)._2))
      }
    }
  }
}
