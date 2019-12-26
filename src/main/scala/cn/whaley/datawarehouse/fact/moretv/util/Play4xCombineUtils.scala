package cn.whaley.datawarehouse.fact.moretv.util

import java.util.Date

import cn.whaley.datawarehouse.global.LogConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.storage.StorageLevel

/**
  * Created by zhu.bingxin on 2018/9/13.
  *
  * 业务作用：合并medusa 4.x play对一次播放开始和一次播放结束日志合为一条记录
  *
  * 基本逻辑：
  * 使用log_uuid 关联一次播放的开始和结束
  * 对相同session unique key进行下面四个字段的判别，剩下的其他字段对于一个play session都一样
  * datetime
  * duration
  * start_event
  * end_event
  * 规则：
  * 1.如果有开始行为又有结束行为，datetime使用开始行为的datetime
  * 2.如果只有开始行为，startplay自己合成一条记录,end_event类型为noEnd
  * 3.如果只有结束行为，结束记录自己合成一条记录,start_event类型为noStart
  *
  * start_event: 【startplay,noStart】
  * end_event    【userexit,selfend,noEnd】
  *
  *
  * 验证：
  * 1.通过where条件指定start_event='startplay'来计算pv,uv,应该和原有统计分析结果一样
  * 2.通过where条件指定end_event in ('userexit','selfend')来计算duration，应该和原有统计分析结果一样
  *
  */
object Play4xCombineUtils extends LogConfig {

  val fact_table_name = "log_data_combine"
  val INDEX_NAME = "r_index"
  val startPlayEvent = "startplay"
  val userExitEvent = "userexit"
  val selfEndEvent = "selfend"
  val noEnd = "noEnd"
  val noStart = "noStart"
  val shortDataFrameTable = "shortDataFrameTable"
  val combineTmpTable = "combineTmpTable"
  var factDataFrameWithIndex: DataFrame = null

  /** 合并操作
    * */
  def get4xCombineDataFrame(factDataFrame: DataFrame, sqlContext: SQLContext): DataFrame = {

    println(new Date + "：Start Combine Data")
    factDataFrame.persist(StorageLevel.MEMORY_AND_DISK_SER)
    println("factDataFrame.count():" + factDataFrame.count())
    factDataFrame.createOrReplaceTempView(fact_table_name)

    /**
      * 1、增加path_str和path_md5字段
      * 2、过滤log_uuid为null的值
      */
    sqlContext.sql(
      s"""
         |select *,rowSeq2String(path) as path_str,md5(rowSeq2String(path)) as path_md5
         |from $fact_table_name
         |where log_uuid is not null
      """.stripMargin).createOrReplaceTempView("filter_data_temp")

    /**
      * 过滤异常播放量
      */
    sqlContext.sql(
      s"""
         |select b.* from
         |(
         |   select user_id,video_sid,realip,path_md5,count(1) as play_num
         |   from filter_data_temp
         |   group by user_id,video_sid,realip,path_md5
         |   having play_num < 5000
         |) a join filter_data_temp b
         |on a.user_id = b.user_id and a.video_sid = b.video_sid  and a.realip = b.realip
         |and a.path_md5 = b.path_md5
      """.stripMargin).createOrReplaceTempView("filter_data")
    //    println("filter_data count is:" + sqlContext.sql("select * from filter_data").count)

    /**
      * 获取最小分区日期
      */
    val minKeyDay = sqlContext.sql(
      s"""
         |select min(key_day) as min_key_day
         |from filter_data where event = '$startPlayEvent'
       """.stripMargin).collect().head.getString(0)
    /**
      * 起播数据
      * 过滤一个log_uuid对应多条起播的数据
      * 过滤多次上传同一条日志
      */

    /*sqlContext.sql(
      s"""
         |select * from
         |(select *,row_number() over (partition by log_uuid order by happen_time) as min_happen_time
         |,row_number() over (partition by log_uuid,happen_time order by logtimestamp) as min_logtimestamp
         |from filter_data where event = '$startPlayEvent') x
         |where happen_time = min_happen_time and logtimestamp = min_logtimestamp
       """.stripMargin).withColumn("min_key_day",lit(minKeyDay)).createOrReplaceTempView("start_table")*/
    sqlContext.sql(
      s"""
         |select * from
         |(select *,row_number() over (partition by user_id,log_uuid order by happen_time) as row_num
         |from filter_data where event = '$startPlayEvent') x
         |where row_num = 1
       """.stripMargin).withColumn("min_key_day",lit(minKeyDay)).createOrReplaceTempView("start_table")
    //println("start_table count is:" + sqlContext.sql("select * from start_table").count)
    /**
      * 结束播放数据
      * 过滤一个log_uuid对应多条结束播放的数据
      * 过滤多次上传同一条日志
      */
    /* sqlContext.sql(
       s"""
          |select * from
          |(select *,row_number() over (partition by log_uuid order by happen_time desc) as max_happen_time
          |,row_number() over (partition by log_uuid,happen_time order by logtimestamp) as min_logtimestamp
          |from filter_data where event != '$startPlayEvent') x
          |where happen_time = max_happen_time and logtimestamp = min_logtimestamp
        """.stripMargin).withColumn("min_key_day",lit(minKeyDay)).createOrReplaceTempView("end_table")*/
    sqlContext.sql(
      s"""
         |select * from
         |(select *,row_number() over (partition by user_id,log_uuid order by happen_time) as row_num
         |from filter_data where event != '$startPlayEvent') x
         |where row_num = 1
       """.stripMargin).withColumn("min_key_day",lit(minKeyDay)).createOrReplaceTempView("end_table")
    //    println("end_table count is:" + sqlContext.sql("select * from end_table").count)
    /**
      * 1、开始播放和结束播放匹配，取开始播放的event为start_event，结束播放的event为end_event，取开始记录的时间和结束记录的时长(附加过滤规则：取今天的开始播放-a.key_day = a.min_key_day)
      * 2、只有开始记录，取开始播放的event为start_event，noEnd作为end_event，取开始记录的时间和时长（附加过滤规则：取今天的开始播放-a.key_day = a.min_key_day）
      * 3、只有结束记录，noStart作为start_event，结束播放的event为end_event，取结束记录的时间和时长（附加过滤规则：结束播放的时间是当天的2-23小时）
      */
    val combineDataFrame = sqlContext.sql(
      s"""
         |select a.*,a.event as start_event,b.event as end_event,a.datetime as fDatetime,b.duration as fDuration
         |from start_table a JOIN end_table b
         |on a.log_uuid = b.log_uuid
         |where a.key_day = a.min_key_day
         |union all
         |select c.*,c.event as start_event,'$noEnd' as end_event,c.datetime as fDatetime,c.duration as fDuration
         |from start_table c LEFT JOIN end_table d
         |on c.log_uuid = d.log_uuid
         |where d.log_uuid is null
         |and c.key_day = c.min_key_day
         |union all
         |select f.*,'$noStart' as start_event,f.event as end_event,f.datetime as fDatetime,f.duration as fDuration
         |from start_table e right JOIN end_table f
         |on e.log_uuid = f.log_uuid
         |where e.log_uuid is null
         |and f.key_hour between '02' and '23'
      """.stripMargin)

    println("combineDataFrame.count():" + combineDataFrame.count())
    combineDataFrame
  }
}
