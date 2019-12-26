package cn.whaley.datawarehouse.fact.moretv.util

import cn.whaley.datawarehouse.global.LogConfig
import cn.whaley.datawarehouse.util.DataFrameUtil
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
  * Created by michael on 2017/5/19.
  *
  * 业务作用：合并medusa 3.x play对一次播放开始和一次播放结束日志为一条记录
  *
  * 基本逻辑：
  * 使用userId,episodeSid,videoSid,pathMain,realIP 作为play session unique key
  * 对相同session unique key进行下面四个字段的判别，剩下的其他字段对于一个play session都一样
  * datetime
  * duration
  * start_event
  * end_event
  * 规则：
  * 1.如果有开始行为又有结束行为，datetime使用开始行为的datetime,并保证合成一条记录的start event datetime小于结束event的datetime
  * 2.如果只有开始行为，startplay自己合成一条记录,end_event类型为noEnd
  * 3.如果只有结束行为，结束记录自己合成一条记录,start_event类型为noStart
  *
  * start_event: 2.x 【playview->startplay 】  3.x【startplay,noStart】
  * end_event    2.x 【playview->unknown 】    3.x【userexit,selfend,noEnd】
  *
  *
  * 验证：
  * 1.通过where条件指定start_event='startplay'来计算pv,uv,应该和原有统计分析结果一样
  * 2.通过where条件指定end_event in ('userexit','selfend')来计算duration，应该和原有统计分析结果一样
  *
  */
object Play3xCombineUtils extends LogConfig {

  val fact_table_name = "log_data_3x_combine"
  val INDEX_NAME = "r_index"
  val startPlayEvent = "startplay"
  val userExitEvent = "userexit"
  val selfEndEvent = "selfend"
  val noEnd = "noEnd"
  val noStart = "noStart"
  val shortDataFrameTable = "shortDataFrameTable"
  val combineTmpTable = "combineTmpTable"
  var factDataFrameWithIndex:DataFrame= null
  /** 合并操作
    * */
  def get3xCombineDataFrame(factDataFrame: DataFrame, sqlContext: SQLContext): DataFrame = {
    //进行索引编号
    factDataFrameWithIndex = DataFrameUtil.dfZipWithIndex(factDataFrame, INDEX_NAME)
    factDataFrameWithIndex.persist(StorageLevel.MEMORY_AND_DISK_SER)
    println("factDataFrameWithIndex.count():" + factDataFrameWithIndex.count())
    factDataFrameWithIndex.registerTempTable(fact_table_name)
    //writeToHDFS(factDataFrameWithIndex, baseOutputPathFact)

    //获得key
    val sqlString =
      s"""select concat_ws('_',userId,episodeSid,videoSid,pathMain,realIP) as key,duration,datetime,event,${INDEX_NAME},'' as start_event
          |from $fact_table_name
       """.stripMargin
    val shortDataFrame = sqlContext.sql(sqlString)
    shortDataFrame.registerTempTable(shortDataFrameTable)
    //(key,list(duration,datetime,event,r_index))
    import scala.util.control.Breaks._
    val rdd1 = shortDataFrame.rdd.map(
      row => (row.getString(0), (if(row.get(1) == null) 0l else row.getLong(1), row.getString(2), row.getString(3), row.getLong(4)))
    ).groupByKey()
    val rddCombineTmp = rdd1
      .filter(x => x._2.size < 5000) //过滤一个人一天观看一个节目超过5000次的数据
      .map(x => {
      val ikey = x._1
      val tupleIterable = x._2
      val startList = tupleIterable.toList.filter(_._3 == startPlayEvent).sortBy(_._2)
      val endList = tupleIterable.toList.filter(_._3 != null).filter(_._3 != startPlayEvent).sortBy(_._2)
      val startLength = startList.length
      val endLength = endList.length
      val arrayBuffer = ArrayBuffer.empty[Row]

      /** 以播放开始行为记录startList为基准遍历：
        * 以播放结束行为记录endList为子循环遍历
        * 如果j已经遍历到endList的尾端
        * 如果播放结束行为datetime大于等于播放开始行为datetime，合成一条日志，并将结束行为索引j存入setJ
        * 否则，继续遍历endList，寻找结束行为datetime大于等于播放开始行为datetime的记录，进行合并，并将结束行为索引j存入setJ
        * 否则，startList里的每条记录自己作为一条合并结果
        * 遍历endList，查看是否所有记录都已经被setJ记录
        * */
      var i = 0
      //用来记录endList中，已经被合并过的记录index
      val setJ = scala.collection.mutable.Set[Int]()
      while (i < startLength) {
        var j = 0
        breakable {
          if (i < endLength) {
            while (j < endLength) {
              if (endList(j)._2 >= startList(i)._2) {
                if (!setJ.contains(j)) {
                  //ikey后面的分别对应结束播放的duration、开始播放的datetime、结束播放的event、结束播放的r_index、开始播放的event
                  val row = Row(ikey, endList(j)._1, startList(i)._2, endList(j)._3, endList(j)._4, startList(i)._3)
                  arrayBuffer.+=(row)
                  setJ.add(j)
                  break
                } else {
                  j = j + 1
                }
              } else {
                j = j + 1
              }
            }
            if (j == endLength) {
              val row = Row(ikey, startList(i)._1, startList(i)._2, noEnd, startList(i)._4, startList(i)._3)
              arrayBuffer.+=(row)
            }
          } else {
            val row = Row(ikey, startList(i)._1, startList(i)._2, noEnd, startList(i)._4, startList(i)._3)
            arrayBuffer.+=(row)
          }
        }
        i = i + 1
      }

      //处理播放结束行为比播放开始行为数量多的情况或者没有被合并的播放结束记录，播放结束每条记录自己合并
      for (k <- 0 until (endLength)) {
        if (!setJ.contains(k)) {
          val row = Row(ikey, endList(k)._1, endList(k)._2, endList(k)._3, endList(k)._4, noStart)
          arrayBuffer.+=(row)
        }
      }

      arrayBuffer.toList
    }).flatMap(x => x)

//    println("shortDataFrame.schema.fields:" + shortDataFrame.schema.fields.foreach(e => println(e.name)))
    val combineDFTmp = sqlContext.createDataFrame(rddCombineTmp, StructType(shortDataFrame.schema.fields))
    combineDFTmp.registerTempTable(combineTmpTable)
    //writeToHDFS(combineDFTmp, baseOutputPathCombineTmp)
    val df = sqlContext.sql(
      s"""select a.*,b.event as end_event,b.start_event as start_event,b.datetime as fDatetime,b.duration as fDuration
          | from   $fact_table_name         a join
          |        $combineTmpTable         b on a.${INDEX_NAME}=b.${INDEX_NAME}
      """.stripMargin)
    df
  }
}
