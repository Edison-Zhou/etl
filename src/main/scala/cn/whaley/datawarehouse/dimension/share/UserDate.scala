package cn.whaley.datawarehouse.dimension.share

import java.text.SimpleDateFormat
import java.util.Calendar

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType.SourceType
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by Tony on 16/12/23.
  *
  * 日期维度ETL
  *
  * 无需自动触发，只需要修改后手动执行一次
  */
object UserDate extends DimensionBase {

  columns.skName = "dim_date_sk"
  columns.primaryKeys = List("date_key")
  columns.trackingColumns = List()
  columns.allColumns = List("date_key",
    "year",
    "month",
    "day_of_month",
    "day_of_year",
    "day_of_week",
    "day_type")
  dimensionName = "dim_date"

  fullUpdate = true

  override def readSource(readSourceType: SourceType): DataFrame = {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    var date = dateFormat.parse("2010-01-01")
    //设置的开始时间
    val endDate = dateFormat.parse("2030-01-01")

    val list = collection.mutable.Buffer[(String, Int, Int, Int, Int, Int, String)]()
    while (date.before(endDate)) {
      val calendar: Calendar = Calendar.getInstance
      calendar.setTime(date)
      val year = calendar.get(Calendar.YEAR)
      val month = calendar.get(Calendar.MONTH) + 1
      val dayOfMonth = calendar.get(Calendar.DAY_OF_MONTH)
      val dayOfYear = calendar.get(Calendar.DAY_OF_YEAR)
      val dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK)
      val dayOfWeekInChina = if (dayOfWeek == 1) 7 else dayOfWeek - 1 //从周日是第一天 转成 周一是第一天

      val row = (dateFormat.format(date), year, month, dayOfMonth, dayOfYear, dayOfWeekInChina, "")
      list.append(row)

      date = DateUtils.addDays(date, 1)
    }

    val schema = {
      val dateKeyFiled = StructField("date_key", StringType)
      val yearFiled = StructField("year", IntegerType)
      val monthField = StructField("month", IntegerType)
      val dayOfMonthField = StructField("day_of_month", IntegerType)
      val dayOfYearField = StructField("day_of_year", IntegerType)
      val dayOfWeekFiled = StructField("day_of_week", IntegerType)
      val dayTypeField = StructField("day_type", StringType)

      new StructType(Array(dateKeyFiled, yearFiled, monthField, dayOfMonthField, dayOfYearField, dayOfWeekFiled, dayTypeField))
    }

    val rdd = sc.makeRDD(list.map(s => Row.fromTuple(s)))

    val df = sqlContext.createDataFrame(rdd, schema)

    df
  }

}
