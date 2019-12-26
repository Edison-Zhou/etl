package cn.whaley.datawarehouse.common

import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * Created by Tony on 17/4/6.
  */
case class UserDefinedColumn(name: String, //新增加的列名
                             udf: UserDefinedFunction, //自定义的解析udf函数
                             inputColumns: List[String], //udf函数的输入参数列名
                             expression: String = null, //sql表达式，跟udf字段只需要一个，expression的优先级低
                             remainInFinal: Boolean = false  //是否保留在最终结果中，在维度表关联时生效
                            ) {

}
