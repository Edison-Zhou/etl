package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB

/**
  * Created by zhangyu on 17/5/13
 *  频道首页的维度表
  */
object PageEntrance extends DimensionBase {

  columns.skName = "page_entrance_sk"
  columns.primaryKeys = List("page_entrance_id")
  columns.trackingColumns = List()
  columns.allColumns = List(
    "page_entrance_id",
    "page_code",
    "page_name",
    "area_code",
    "area_name",
    "location_code",
    "location_name",
    "location_index")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "page_entrance_id" -> "id"
  )


  sourceDb = MysqlDB.dwDimensionDb("whaley_page_entrance")

  dimensionName = "dim_whaley_page_entrance"
}
