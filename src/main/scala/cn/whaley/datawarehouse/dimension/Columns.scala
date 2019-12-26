package cn.whaley.datawarehouse.dimension

import cn.whaley.datawarehouse.common.{DimensionColumn, UserDefinedColumn}

/**
  * Created by Tony on 17/3/8.
  *
  * 配置维度表的字段信息
  */
class Columns {

  /**
    * 代理键
    */
  var skName: String = "sk"

  /**
    * 业务键，可以是联合主键
    */
  var primaryKeys: List[String] = _

  /**
    * 需要追踪历史记录的列，记录所有的非null历史记录
    * 可以自由增加或减少列
    */
  var trackingColumns: List[String] = List()

  /**
    * 对应源数据表的所有列，包括业务键和追踪历史记录的列
    * 可以在维度生成或自由增加或减少列
    */
  var allColumns: List[String] = _

  /**
    * 生效时间
    */
  var validTimeKey: String = "dim_valid_time"

  /**
    * 失效时间
    */
  var invalidTimeKey: String = "dim_invalid_time"

  /**
    * 通过解析增加的字段
    */
  var addColumns: List[UserDefinedColumn] = _

  /**
    * 关联的维度
    */
  var linkDimensionColumns: List[DimensionColumn] = _

  /**
    * 获取需要从源数据中获取信息的列
    *
    * @return
    */
  def getSourceColumns: List[String] = {
    allColumns
  }

  def getOtherColumns: List[String] = {
    var columns = allColumns
    if(primaryKeys != null) {
      primaryKeys.foreach(e=> columns = columns.drop(columns.indexOf(e)))
    }
    if(trackingColumns != null) {
      trackingColumns.foreach(e=> columns = columns.drop(columns.indexOf(e)))
    }
    columns
  }

}

