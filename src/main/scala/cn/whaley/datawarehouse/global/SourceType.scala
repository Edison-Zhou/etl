package cn.whaley.datawarehouse.global

/**
  * Created by Tony on 17/3/8.
  *
  * 读取源数据的来源
  */
object SourceType extends Enumeration{
  type SourceType = Value
  val jdbc = Value
  val parquet = Value
  val ods = Value
  val ods_parquet = Value
  val hive = Value
  val hive_parquet = Value
  val custom = Value  //自定义获取源数据
}
