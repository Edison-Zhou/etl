package cn.whaley.datawarehouse.util

import scala.collection.mutable

/**
  * Created by baozhiwang on 2017/3/6.
  */

case class Params(
                   isOnline: Boolean = false, //参数决定维度数据是否上线
                   startDate: String = null,
                   startHour: String = null,
                   endDate: String = null,
                   debug: Boolean = false, //打印调试信息
                   mode: String = null, // 数据更新逻辑
                   isRefresh: Boolean = false, // 是否需要重新刷历史数据
                   factId: String = null,
                   jobName: String = null,
                   paramMap: mutable.Map[String, Any] = mutable.Map[String, Any](), //其他非命令行参
                   factColumn: Seq[String] = Seq() //传入事实表中需要更新的维度，多个以逗号分隔
                 )

