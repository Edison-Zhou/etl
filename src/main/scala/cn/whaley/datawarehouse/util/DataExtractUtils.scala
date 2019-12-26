package cn.whaley.datawarehouse.util

import cn.whaley.datawarehouse.fact.constant.LogPath
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by Tony on 17/4/15.
  */
object DataExtractUtils {

  def readFromJdbc(sqlContext: SQLContext, sourceDb: Map[String, String]): DataFrame = {
    sqlContext.read.format("jdbc").options(sourceDb).load()
  }

  def getParquetPath(sourceParquetPath: String, startDate: String): String = {
    sourceParquetPath.replace(LogPath.DATE_ESCAPE, startDate)
  }

  def readFromParquet(sqlContext: SQLContext, sourceParquetPath: String, startDate: String): DataFrame = {
    val filePath = sourceParquetPath.replace(LogPath.DATE_ESCAPE, startDate)
    val sourceDf = sqlContext.read.parquet(filePath)
    sourceDf
  }

  def readFromParquet(sqlContext: SQLContext, sourceParquetPath: String): DataFrame = {
    val sourceDf = sqlContext.read.parquet(sourceParquetPath)
    sourceDf
  }

  def readFromOds(sqlContext: SQLContext, tableName: String, startDate: String, startHour: String): DataFrame = {
    val sql = s"select * from $tableName where key_day = '$startDate'" +
      (if (startHour != null) s" and key_hour = '$startHour'" else "")
    val sourceDf = sqlContext.sql(sql)
    sourceDf
  }

  def readFromOdsParquet(sqlContext: SQLContext, tableName: String, startDate: String, startHour: String): DataFrame = {
    val pathName = if(tableName.contains(".")) {
      tableName.split("\\.").last
    } else {
      tableName
    }
    val path =
      if (startHour != null)
        s"/data_warehouse/ods_view.db/$pathName/key_day=$startDate/key_hour=$startHour"
      else
        s"/data_warehouse/ods_view.db/$pathName/key_day=$startDate/*"
    val sourceDf = sqlContext.read.parquet(path)
    sourceDf
  }

  def readFromFactParquet(sqlContext: SQLContext, tableName: String, source: String, startDate: String, startHour: String): DataFrame = {
    val pathName = if (tableName.contains(".")) {
      tableName.split("\\.").last
    } else {
      tableName
    }
    val path =
      if (startHour != null)
        s"/data_warehouse/dw_dm/event/$pathName/day_p=$startDate/hour_p=$startHour/source_p=$source"
      else
        s"/data_warehouse/dw_dm/event/$pathName/day_p=$startDate/source_p=$source/*"
    val sourceDf = sqlContext.read.parquet(path)
    sourceDf
  }

  def readFromFact(sqlContext: SQLContext, tableName: String, startDate: String, startHour: String): DataFrame = {
    val table = if(tableName.contains(".")){
      tableName
    }else{
      "dw_facts."+tableName
    }
    val sql = s"SELECT * FROM $table WHERE day_p='$startDate'" +
      (if (startHour != null) s" and day_hour='$startHour'" else "")
    val sourceDf = sqlContext.sql(sql)
    sourceDf
  }

  def readFromDimension(sqlContext: SQLContext, tableName: String): DataFrame = {
    val sql = s"SELECT * FROM dw_dimensions.$tableName"
    val sourceDf = sqlContext.sql(sql)
    sourceDf
  }

}
