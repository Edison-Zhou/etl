package cn.whaley.datawarehouse.market.dim.medusa

import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.market.dim.DmDimensionBase
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame


/**
  * Created by zhu.bingxin on 19/9/11.
  *
  * 集市层维度表
  */
object Account extends DmDimensionBase {
  columns.skName = "account_sk"
  columns.primaryKeys = List("accountId")
  columns.trackingColumns = List()
  columns.allColumns = List("accountId", "email", "mobile", "sex", "birthday", "reg_time", "reg_source", "reg_channel", "is_vip", "is_mtvip", "mtvip_start_time", "mtvip_effective_time", "mtvip_create_time", "is_yueting", "yueting_start_time", "yueting_effective_time", "yueting_create_time")

  readSourceType = jdbc

  //维度表的字段对应源数据的获取方式
  sourceColumnMap = Map(
    "accountId" -> "moretv_id",
    "email" -> "email",
    "mobile" -> "mobile",
    "sex" -> "sex",
    "birthday" -> "birthday",
    "reg_time" -> "cast(create_time as timestamp)",
    "reg_source" -> "register_source",
    "reg_channel" -> "register_channel",
    "is_vip" -> "is_vip",
    "is_mtvip" -> "is_mtvip",
    "mtvip_start_time" -> "mtvip_start_time",
    "mtvip_effective_time" -> "mtvip_effective_time",
    "mtvip_create_time" -> "mtvip_create_time",
    "is_yueting" -> "is_yueting",
    "yueting_start_time" -> "yueting_start_time",
    "yueting_effective_time" -> "yueting_effective_time",
    "yueting_create_time" -> "yueting_create_time"
  )

  sourceFilterWhere = "accountId is not null and accountId <> ''"
  val accountDB = MysqlDB.medusaAccountDB("account")
  val registerDB = MysqlDB.medusaAccountDB("register")
  val accountProfileDB = MysqlDB.medusaAccountDB("account_profile")
  val memberUserAuthorityDB = MysqlDB.medusaMemberDB("member_user_authority")

  dimensionName = "dim_medusa_account"

  fullUpdate = true

  override def readSource(readSourceType: Value): DataFrame = {
    val accountDF = sqlContext.read.format("jdbc").options(accountDB).load()
    val registerDF = sqlContext.read.format("jdbc").options(registerDB).load()
    val accountProfileDF = sqlContext.read.format("jdbc").options(accountProfileDB).load()
    val memberUserAuthorityDF = sqlContext.read.format("jdbc").options(memberUserAuthorityDB).load()

    registerDF.join(accountDF, accountDF("moretv_id") === registerDF("moretv_id"), "left_outer")
      .join(accountProfileDF, registerDF("moretv_id") === accountProfileDF("moretv_id"), "left_outer")
      .select(registerDF("moretv_id"), accountDF("email"), accountDF("mobile"),
        accountProfileDF("username"), accountProfileDF("sex"), accountProfileDF("birthday"),
        registerDF("create_time"), registerDF("register_source"), registerDF("register_channel"), registerDF("register_ip"))
      .createTempView("account_temp01")
    memberUserAuthorityDF.createTempView("member")

    sqlContext.sql(
      """
        |select a.*
        |,if(b.account_id is not null,1,0) as is_vip
        |,if(b.member_code is not null,1,0) as is_mtvip
        |,if(b.member_code is not null,b.start_time,null) as mtvip_start_time
        |,if(b.member_code is not null,b.effective_time,null) as mtvip_effective_time
        |,if(b.member_code is not null,b.create_time,null) as mtvip_create_time
        |,if(b.member_name is not null,1,0) as is_yueting
        |,if(b.member_name is not null,b.start_time,null) as yueting_start_time
        |,if(b.member_name is not null,b.effective_time,null) as yueting_effective_time
        |,if(b.member_name is not null,b.create_time,null) as yueting_create_time
        |from account_temp01 a left join
        |(select * from member where status = 1 and member_code = 'MTVIP') b
        |on a.moretv_id = b.account_id left join
        |(select * from member where status = 1 and member_name = '悦厅会员') c
        |on a.moretv_id = c.account_id
      """.stripMargin)
  }
}
