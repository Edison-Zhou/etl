package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame


/**
  * Created by huanghu on 17/3/15.
  *
  * 微鲸端账号信息
  */
object Account extends DimensionBase {
  columns.skName = "account_sk"
  columns.primaryKeys = List("account_id")
  columns.trackingColumns = List()
  columns.allColumns = List("account_id", "uid", "user_name", "password", "email", "reg_ip",
    "reg_time", "last_login_ip", "last_login_time", "status",
    "union_id", "mobile", "email_pwd", "register_from", "gender", "device", "birthday", "update_time")


  sourceDb = MysqlDB.whaleyAccount("bbs_ucenter_helios_members", "uid", 1, 1000000000, 10)

  dimensionName = "dim_whaley_account"

  fullUpdate = true

  override def filterSource(sourceDf: DataFrame): DataFrame = {

    sourceDf.registerTempTable("bbs_ucenter_helios_members")

    sqlContext.sql("select uid,heliosid,username,password,email,regip," +
      "from_unixtime(regdate) as reg_time,lastloginip,from_unixtime(lastlogintime) as last_login_time," +
      "status,unionid,mobile,emailpwd,registerfrom " +
      "from `bbs_ucenter_helios_members` where uid is not null and heliosid is not null and heliosid <> '' ").registerTempTable("userMembers")


    val userMemberDb = MysqlDB.whaleyAccount("bbs_ucenter_helios_memberfields", "uid", 1, 1000000000, 10)
    sqlContext.read.format("jdbc").options(userMemberDb).load().registerTempTable("bbs_ucenter_helios_memberfields")
    sqlContext.sql("select uid,gender,device,birthday,from_unixtime(update_time) as update_time from `bbs_ucenter_helios_memberfields`" +
      " where uid is not null ").registerTempTable("userMeberfields")

    sqlContext.sql("select a.uid as uid, a.heliosid as account_id ,a.username as user_name," +
      " a.password as password, a.email as email,a.regip as reg_ip,a.reg_time as reg_time, a.lastloginip as last_login_ip, " +
      "a.last_login_time as last_login_time,a.status as status,a.unionid as union_id,a.mobile as mobile,a.emailpwd as email_pwd," +
      "a.registerfrom as register_from,b.gender as gender ,b.device as device,b.birthday as birthday,b.update_time as update_time" +
      " from userMembers a left join userMeberfields b on a.uid = b.uid")

  }
}
