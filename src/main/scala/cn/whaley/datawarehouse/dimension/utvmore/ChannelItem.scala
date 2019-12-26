package cn.whaley.datawarehouse.dimension.utvmore

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB._
import org.apache.spark.sql.{Dataset, Row}

/**
  * Created by Zhu.bingxin on 2019/06/18.
  * 优视猫频道节目单维度表（包括频道组、频道及节目单信息）
  * 目前只有轮播节目
  */
object ChannelItem extends DimensionBase {

  columns.skName = "channel_item_sk"
  columns.primaryKeys = List("id", "channel_group_code")
  columns.trackingColumns = List()
  columns.allColumns = List(
    "id",
    "title",
    "sid",
    "eid",
    "vid",
    "link_type",
    "channel_sid",
    "channel_name",
    "channel_index",
    "channel_type",
    "channel_group_code",
    "channel_group_title")

  readSourceType = jdbc


  val SourceType = jdbc

  /**
    * 读取优视猫cms中的表
    */
  //节目单信息表
  val sourceMtvChannelItem = utvmoreGeminiTvservice("mtv_channel_item", "id", 1, 3000, 20)
  //频道信息表
  val sourceMtvChannel = utvmoreGeminiTvservice("mtv_channel", "id", 1, 3000, 20)
  //频道组信息表
  val sourceMtvChannelGroup = utvmoreGeminiTvservice("mtv_channel_group", "id", 1, 3000, 20)
  //频道和频道组关联表
  val sourceMtvRelChannelGroup = utvmoreGeminiTvservice("mtv_rel_channel_group", "id", 1, 3000, 20)


  dimensionName = "dim_utvmore_channel_item"

  fullUpdate = true


  override def readSource(SourceType: Value): Dataset[Row] = {

    //读取相应配置的表
    sqlContext.read.format("jdbc").options(sourceMtvChannelItem).load.createTempView("mtv_channel_item")
    sqlContext.read.format("jdbc").options(sourceMtvChannel).load.createTempView("mtv_channel")
    sqlContext.read.format("jdbc").options(sourceMtvChannelGroup).load.createTempView("mtv_channel_group")
    sqlContext.read.format("jdbc").options(sourceMtvRelChannelGroup).load.createTempView("mtv_rel_channel_group")


    /**
      * 逻辑处理，主要是关联操作
      * 取出的字段分别对应：节目单的id、名称、sid、eid、vid、关联类型，频道的sid、名称、序号、类型，频道组的code、名称
      */
    sqlContext.sql(
      """
        |SELECT a.id,a.title,a.`link_sid` as sid,a.`link_eid` as eid,a.`link_value` as vid,a.`link_type`
        |,b.`station_code` as channel_sid,b.`station` as channel_name,b.`index` as channel_index,b.type as channel_type
        |,d.`code` as channel_group_code,d.`title` as channel_group_title
        |FROM mtv_channel_item a
        |join mtv_channel b
        |on a.`channel_id` = b.`id`
        |join `mtv_rel_channel_group` c
        |on b.id = c.`channel_id`
        |join `mtv_channel_group` d
        |on c.`group_id` = d.id
        |where a.`status` = 1 and b.`status` = 1 and c.`status` = 1 and d.`status` = 1
      """.stripMargin).createTempView("channel_item")

    /**
      * 增加观看历史频道组后，作为最终的输出
      */
    val result = sqlContext.sql(
      """
        |select distinct id,title,sid,eid,vid,link_type,channel_sid,channel_name,channel_index,channel_type
        |,'history' as channel_group_code,'观看历史' as channel_group_title
        |from channel_item
        |union all
        |select * from channel_item
      """.stripMargin)


    result
  }


}
