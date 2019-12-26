package cn.whaley.datawarehouse.common

/**
  * Created by Tony on 17/4/18.
  */
case class DimensionJoinCondition(columnPairs: Map[String, String],
                                  whereClause: String = null,//维度表过滤
                                  orderBy: List[(String, Boolean)] = null, //元组中，第一个是排序字段名，第二个是排序方式，true表示倒序
                                  sourceWhereClause: String = null
                                 ) {

  def this(columnPairs: Map[String, String]) = {
    this(columnPairs, null, null)
  }

  def this(columnPairs: Map[String, String], whereClause: String) = {
    this(columnPairs, whereClause, null)
  }

  def this(columnPairs: Map[String, String], orderBy: List[(String, Boolean)]) = {
    this(columnPairs, null, orderBy)
  }
}
