package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame


/**
  * Created by Tony on 17/3/8.
  *
  * 电视猫账号维度表
  */
object Account extends DimensionBase {
  columns.skName = "account_sk"
  columns.primaryKeys = List("account_id")
  columns.trackingColumns = List()
  columns.allColumns = List("account_id", "email", "mobile", "user_name", "sex", "birthday", "reg_time", "reg_source", "reg_channel", "reg_ip")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "account_id" -> "moretv_id",
    "email" -> "email",
    "mobile" -> "mobile",
    "user_name" -> "username",
    "sex" -> "sex",
    "birthday" -> "birthday",
    "reg_time" -> "cast(create_time as timestamp)",
    "reg_source" -> "register_source",
    "reg_channel" -> "register_channel",
    "reg_ip" -> "register_ip"
  )

  sourceFilterWhere = "account_id is not null and account_id <> ''"
  val accountDB = MysqlDB.medusaAccountDB("account")
  val registerDB = MysqlDB.medusaAccountDB("register")
  val accountProfileDB = MysqlDB.medusaAccountDB("account_profile")

  dimensionName = "dim_medusa_account"

  fullUpdate = true

  override def readSource(readSourceType: Value): DataFrame = {
    val accountDF = sqlContext.read.format("jdbc").options(accountDB).load()
    val registerDF = sqlContext.read.format("jdbc").options(registerDB).load()
    val accountProfileDF = sqlContext.read.format("jdbc").options(accountProfileDB).load()

    registerDF.join(accountDF, accountDF("moretv_id") === registerDF("moretv_id"), "left_outer").
      join(accountProfileDF, registerDF("moretv_id") === accountProfileDF("moretv_id"), "left_outer").
      select(registerDF("moretv_id"), accountDF("email"), accountDF("mobile"),
             accountProfileDF("username"), accountProfileDF("sex"), accountProfileDF("birthday"),
             registerDF("create_time"), registerDF("register_source"), registerDF("register_channel"), registerDF("register_ip"))

  }
}
