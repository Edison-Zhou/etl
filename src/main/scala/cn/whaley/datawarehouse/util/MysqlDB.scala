package cn.whaley.datawarehouse.util

/**
  * Created by Tony on 16/12/26.
  */
object MysqlDB {

  def medusaUCenterMember = {
    Map("url" -> "jdbc:mysql://bigdata-extsvr-db_moretv_ucenter:3306/ucenter?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> "bbs_ucenter_members",
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "bislave",
      "password" -> "slave4bi@whaley",
      "partitionColumn" -> "uid",
      "lowerBound" -> "1",
      "upperBound" -> "4619253",
      "numPartitions" -> "10")
  }

  def medusaAccountDB(table:String) = {
    Map("url" -> "jdbc:mysql://10.10.59.240:3306/moretv_account?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> table,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "bigdata",
      "password" -> "bigdata2017!@#")
  }

  def medusaMemberDB(table:String) = {
    Map("url" -> "jdbc:mysql://10.10.57.213:3306/moretv_admin?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> table,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "bislave",
      "password" -> "slave4bi@whaley",
      "partitionColumn" -> "id",
      "lowerBound" -> "1",
      "upperBound" -> "400000000",
      "numPartitions" -> "400")
  }

  def utvmoreMemberDB(table:String) = {
    Map("url" -> "jdbc:mysql://10.10.63.155:3306/youshimao_member?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> table,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "readonly",
      "password" -> "readonly",
      "partitionColumn" -> "id",
      "lowerBound" -> "1",
      "upperBound" -> "400000000",
      "numPartitions" -> "400")
  }

  def medusaTvServiceAccount = {
    Map("url" -> "jdbc:mysql://bigdata-appsvr-130-3:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> "mtv_account",
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "bislave",
      "password" -> "slave4bi@whaley",
      "partitionColumn" -> "id",
      "lowerBound" -> "1",
      "upperBound" -> "800000000",
      "numPartitions" -> "800")
  }

  def medusaProgramInfo(table: String) = {
    Map("url" -> "jdbc:mysql://bigdata-appsvr-130-4:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> table,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "bislave",
      "password" -> "slave4bi@whaley")
  }

  def medusaProgramInfo(table: String, partitionColumn: String, lowerBound: Long, upperBound: Long, numPartitions: Int) = {
    Map("url" -> "jdbc:mysql://bigdata-appsvr-130-4:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> table,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "bislave",
      "password" -> "slave4bi@whaley",
      "partitionColumn" -> partitionColumn,
      "lowerBound" -> lowerBound.toString,
      "upperBound" -> upperBound.toString,
      "numPartitions" -> numPartitions.toString)
  }

  def medusaSmallVideoProgramInfo(table: String, partitionColumn: String, lowerBound: Long, upperBound: Long, numPartitions: Int) = {
    Map("url" -> "jdbc:mysql://10.19.36.65:3306/europa?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> table,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "readonly",
      "password" -> "readonly",
      "partitionColumn" -> partitionColumn,
      "lowerBound" -> lowerBound.toString,
      "upperBound" -> upperBound.toString,
      "numPartitions" -> numPartitions.toString)
  }

  def medusaCms(table: String, partitionColumn: String, lowerBound: Long, upperBound: Long, numPartitions: Int) = {
    Map("url" -> "jdbc:mysql://bigdata-appsvr-130-2:3306/mtv_cms?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> table,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "bislave",
      "password" -> "slave4bi@whaley",
      "partitionColumn" -> partitionColumn,
      "lowerBound" -> lowerBound.toString,
      "upperBound" -> upperBound.toString,
      "numPartitions" -> numPartitions.toString)
  }
  def kidseduCms(table: String, partitionColumn: String, lowerBound: Long, upperBound: Long, numPartitions: Int) ={
    Map("url" -> "jdbc:mysql://10.19.180.144:3306/aries_cms?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> table,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "readonly",
      "password" -> "readonly",
      "partitionColumn" -> partitionColumn,
      "lowerBound" -> lowerBound.toString,
      "upperBound" -> upperBound.toString,
      "numPartitions" -> numPartitions.toString)
  }

  def ams(table: String): Map[String,String] = {
    Map("url" -> "jdbc:mysql://10.19.196.189:3306/ams?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> table,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "readonly",
      "password" -> "readonly")
  }

  def medusaTvService(table: String, partitionColumn: String, lowerBound: Long, upperBound: Long, numPartitions: Int) = {
    Map("url" -> "jdbc:mysql://bigdata-appsvr-130-4:3306/tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> table,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "bislave",
      "password" -> "slave4bi@whaley",
      "partitionColumn" -> partitionColumn,
      "lowerBound" -> lowerBound.toString,
      "upperBound" -> upperBound.toString,
      "numPartitions" -> numPartitions.toString)
  }

  def dwDimensionDb(table: String) = {
    Map("url" -> "jdbc:mysql://bigdata-extsvr-db_bi2:3306/dw_dimension?useUnicode=true&characterEncoding=utf-8&autoReconnect=true&zeroDateTimeBehavior=convertToNull",
      "dbtable" -> table, //"moretv_app_version",
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "bi",
      "password" -> "mlw321@moretv")
  }


  def whaleyCms(table: String, partitionColumn: String, lowerBound: Long, upperBound: Long, numPartitions: Int) = {
    Map("url" -> "jdbc:mysql://bigdata-appsvr-130-1:3306/mtv_cms?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> table,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "bislave",
      "password" -> "slave4bi@whaley",
      "partitionColumn" -> partitionColumn,
      "lowerBound" -> lowerBound.toString,
      "upperBound" -> upperBound.toString,
      "numPartitions" -> numPartitions.toString)
  }

  def whaleyDolphin(table: String, partitionColumn: String, lowerBound: Long, upperBound: Long, numPartitions: Int) = {
    Map("url" -> "jdbc:mysql://bigdata-extsvr-db_whaley_dlph_tmnl:3306/dolphin_terminal?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> table,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "bislave",
      "password" -> "slave4bi@whaley",
      "partitionColumn" -> partitionColumn,
      "lowerBound" -> lowerBound.toString,
      "upperBound" -> upperBound.toString,
      "numPartitions" -> numPartitions.toString)
  }

  //  def whaleyDimension(table: String, partitionColumn: String, lowerBound: Long, upperBound: Long, numPartitions:Int ) = {
  //    Map("url" -> "jdbc:mysql://10.10.2.16:3306/dw_dimension?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
  //      "dbtable" -> table,
  //      "driver" -> "com.mysql.jdbc.Driver",
  //      "user" -> "dw_user",
  //      "password" -> "dw_user@whaley",
  //      "partitionColumn" -> partitionColumn,
  //      "lowerBound" -> lowerBound.toString,
  //      "upperBound" -> upperBound.toString,
  //      "numPartitions" -> numPartitions.toString)
  //  }

  def whaleyAccount(table: String, partitionColumn: String, lowerBound: Long, upperBound: Long, numPartitions: Int) = {
    Map("url" -> "jdbc:mysql://bigdata-extsvr-db_whaley_ucenter:3306/ucenter?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> table,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "bislave",
      "password" -> "slave4bi@whaley",
      "partitionColumn" -> partitionColumn,
      "lowerBound" -> lowerBound.toString,
      "upperBound" -> upperBound.toString,
      "numPartitions" -> numPartitions.toString)
  }


  def whaleyTerminalMember = {
    Map("url" -> "jdbc:mysql://bigdata-extsvr-db_whaley_tmnl_upg:3306/terminal_upgrade?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> "mtv_terminal",
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "bislave",
      "password" -> "slave4bi@whaley",
      "partitionColumn" -> "id",
      "lowerBound" -> "1",
      "upperBound" -> "25000000",
      "numPartitions" -> "100")
  }


  def mergerActivity = {
    Map("url" -> "jdbc:mysql://bigdata-extsvr-db_bi1:3306/eagletv?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> "mtv_activity",
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "bi",
      "password" -> "mlw321@moretv",
      "partitionColumn" -> "sid",
      "lowerBound" -> "1",
      "upperBound" -> "50",
      "numPartitions" -> "2"
    )
  }

  def whaleyApp(table: String, partitionColumn: String, lowerBound: Long, upperBound: Long, numPartitions: Int) = {
    Map("url" -> "jdbc:mysql://10.10.72.124:3306/app_cms?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> table,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "biread",
      "password" -> "bigdataTV@608_810",
      "partitionColumn" -> partitionColumn,
      "lowerBound" -> lowerBound.toString,
      "upperBound" -> upperBound.toString,
      "numPartitions" -> numPartitions.toString)
  }

  def programTag(table: String, partitionColumn: String, lowerBound: Long, upperBound: Long, numPartitions: Int) = {
    Map("url" -> "jdbc:mysql://bigdata-appsvr-130-6:3306/europa?useUnicode=true&characterEncoding=utf-8&autoReconnect=true&tinyInt1isBit=false",
      "dbtable" -> table,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "bislave",
      "password" -> "slave4bi@whaley",
      "partitionColumn" -> partitionColumn,
      "lowerBound" -> lowerBound.toString,
      "upperBound" -> upperBound.toString,
      "numPartitions" -> numPartitions.toString)
  }

  def youkuProgramTag(table: String, partitionColumn: String, lowerBound: Long, upperBound: Long, numPartitions: Int) = {
    Map("url" -> "jdbc:mysql://bigtest-cmpt-129-204:3306/europa?useUnicode=true&characterEncoding=utf-8&autoReconnect=true&tinyInt1isBit=false",
      "dbtable" -> table,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "root",
      "password" -> "root1234",
      "partitionColumn" -> partitionColumn,
      "lowerBound" -> lowerBound.toString,
      "upperBound" -> upperBound.toString,
      "numPartitions" -> numPartitions.toString)
  }

  //长连接用户信息
  def lcmsAccount = {
    Map("url" -> "jdbc:mysql://10.19.100.243:3306/lcms_base?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> "account",
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "lcmsreade",
      "password" -> "moretv2016!@#",
      "partitionColumn" -> "id",
      "lowerBound" -> "1",
      "upperBound" -> "7000000000",
      "numPartitions" -> "1000")
  }

  //销售数据库中的相关信息
  def whaleyWms(table: String, partitionColumn: String, lowerBound: Long, upperBound: Long, numPartitions: Int) = {
    Map("url" -> "jdbc:mysql://10.19.35.248:3306/whaley_wms?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> table,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "bislave",
      "password" -> "slave4bi@whaley",
      "partitionColumn" -> partitionColumn,
      "lowerBound" -> lowerBound.toString,
      "upperBound" -> upperBound.toString,
      "numPartitions" -> numPartitions.toString)
  }

  //电视猫猫优选商城的相关信息
  def medusaVenus(table: String, partitionColumn: String, lowerBound: Long, upperBound: Long, numPartitions: Int) = {
    Map("url" -> "jdbc:mysql://10.19.141.52:3306/venus?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> table,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "venus",
      "password" -> "venus654321!@#",
      "partitionColumn" -> partitionColumn,
      "lowerBound" -> lowerBound.toString,
      "upperBound" -> upperBound.toString,
      "numPartitions" -> numPartitions.toString)
  }

  //优视猫节目从库节目相关信息
  def utvmoreGeminiTvservice(table: String, partitionColumn: String, lowerBound: Long, upperBound: Long, numPartitions: Int) = {
    Map("url" -> "jdbc:mysql://10.10.195.241:3306/gemini_tvservice?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> table,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "bi_geminiTvservice",
      "password" -> "bi_geminiTvservice",
      "partitionColumn" -> partitionColumn,
      "lowerBound" -> lowerBound.toString,
      "upperBound" -> upperBound.toString,
      "numPartitions" -> numPartitions.toString)
  }

  //优视猫节目从库节目相关信息
  def utvmoreGeminiCms(table: String, partitionColumn: String, lowerBound: Long, upperBound: Long, numPartitions: Int) = {
    Map("url" -> "jdbc:mysql://10.19.193.160:3306/gemini_cms?useUnicode=true&characterEncoding=utf-8&autoReconnect=true",
      "dbtable" -> table,
      "driver" -> "com.mysql.jdbc.Driver",
      "user" -> "readonly",
      "password" -> "readonly",
      "partitionColumn" -> partitionColumn,
      "lowerBound" -> lowerBound.toString,
      "upperBound" -> upperBound.toString,
      "numPartitions" -> numPartitions.toString)
  }
}
