package cn.whaley.datawarehouse.common

/**
  * Created by lituo on 2017/9/20.
  */
object Udf {

  def getIpKey(ip: String): Long = {
    try {
      val ipInfo = ip.split("\\.")
      if (ipInfo.length >= 3) {
        (((ipInfo(0).toLong * 256) + ipInfo(1).toLong) * 256 + ipInfo(2).toLong) * 256
      } else 0
    } catch {
      case ex: Exception => 0
    }
  }

  def getDimDate(dateTime: String): String = {
    try {
      val dateTimeInfo = dateTime.split(" ")
      if (dateTimeInfo.length >= 2) {
        dateTimeInfo(0)
      } else ""
    } catch {
      case ex: Exception => ""
    }
  }

  def getDimTime(dateTime: String): String = {
    try {
      val dateTimeInfo = dateTime.split(" ")
      if (dateTimeInfo.length >= 2) {
        dateTimeInfo(1)
      } else ""
    } catch {
      case ex: Exception => ""
    }
  }

  def getAppSeries(seriesAndVersion: String): String = {
    try {
      val index = seriesAndVersion.lastIndexOf("_")
      if (index > 0) {
        seriesAndVersion.substring(0, index)
      } else ""
    } catch {
      case ex: Exception => ""
    }
  }

  def getAppVersion(seriesAndVersion: String): String = {
    try {
      val index = seriesAndVersion.lastIndexOf("_")
      if (index > 0) {
        seriesAndVersion.substring(index + 1, seriesAndVersion.length)
      } else ""
    } catch {
      case ex: Exception => ""
    }
  }
}
