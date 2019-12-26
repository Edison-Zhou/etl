package cn.whaley.datawarehouse.common

/**
  * Created by Tony on 17/4/6.
  */
case class DimensionColumn(dimensionName: String,

                           /**
                             * 维度别名，不能重复
                             */
                           dimensionNameAs: String,

                           /**
                             * list内的是或关系
                             */
                           joinColumnList: List[DimensionJoinCondition],

                           /**
                             * 维度表的sk名称
                             */
                           dimensionSkName: String,

                           /**
                             * 关联到事实表中的sk名
                             */
                           factSkColumnName: String,

                           /**
                             * 源数据和维度表join后从维度表中拿的字段列表，包含sk字段
                             * 元组格式（维度表中的字段名，获取到实时表中的命名）
                             */
                           dimensionColumnName: List[(String, String)]
                          ) {


  def this(dimensionName: String,
           joinColumnList: List[DimensionJoinCondition],
           dimensionSkName: String) =
    this(dimensionName, dimensionName, joinColumnList,dimensionSkName,dimensionSkName, List((dimensionSkName, dimensionSkName)))

  def this(dimensionName: String,
           joinColumnList: List[DimensionJoinCondition],
           dimensionSkName: String,
           factSkColumnName: String) =
    this(dimensionName, dimensionName, joinColumnList,dimensionSkName, factSkColumnName, List((dimensionSkName, factSkColumnName)))


  def this(dimensionName: String,
           dimensionNameAs: String,
           joinColumnList: List[DimensionJoinCondition],
           dimensionSkName: String,
           factSkColumnName: String) =
    this(dimensionName, dimensionNameAs, joinColumnList,dimensionSkName, factSkColumnName, List((dimensionSkName, factSkColumnName)))

  /**
    * 构造函数
    * @param dimensionName  维度名称
    * @param joinColumnList 需要关联的列
    * @param dimensionColumnName 从维度中获取的字段
    * @return
    */
  def this(dimensionName: String,
           joinColumnList: List[DimensionJoinCondition],
           dimensionColumnName: List[(String, String)]) =
    this(dimensionName, dimensionName, joinColumnList, dimensionColumnName.head._1,dimensionColumnName.head._2, dimensionColumnName)
}
