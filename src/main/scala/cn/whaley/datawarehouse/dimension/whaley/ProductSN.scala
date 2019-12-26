package cn.whaley.datawarehouse.dimension.whaley


import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition, UserDefinedColumn}
import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.fact.whaley.util.RomVersionUtils
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.storage.StorageLevel

/**
  * Created by huanghu on 17/3/14.
  *
  * 电视productSN维度表
  */
object ProductSN extends DimensionBase {

  columns.skName = "product_sn_sk"
  columns.primaryKeys = List("product_sn")
  columns.trackingColumns = List("rom_version","wui_version")
  columns.allColumns = List("product_sn", "product_line", "product_model", "user_id", "rom_version", "wui_version",
    "mac", "open_time", "sold_time", "login_time","wifi_mac", "ip", "vip_type",
    "country", "area", "province", "city", "district", "isp", "city_level", "prefecture_level_city")

  columns.addColumns = List(
    UserDefinedColumn("ip_key", udf(getIpKey: String => Long), List("ip"))
  )

  columns.linkDimensionColumns = List(
    new DimensionColumn(
      "dim_web_location",
      List(DimensionJoinCondition(Map("ip_key" -> "web_location_key"))),
      "web_location_sk"
    )
  )

  sourceDb = MysqlDB.whaleyTerminalMember


  dimensionName = "dim_whaley_product_sn"

  sourceTimeCol = "login_time"

  override def filterSource(sourceDf: DataFrame): DataFrame = {


    sourceDf.registerTempTable("mtv_terminal")

    sqlContext.udf.register("wui", RomVersionUtils.getRomVersion _)
    //得出用户的信息，status表示用户是否有效，activate_status表示用户是否激活
    //限制id != 1339211 是因为有一条重复数据先去掉（此处为临时解决方案）
    sqlContext.sql("select  serial_number, service_id,rom_version, wui(wui_version,rom_version) as wui_version ,mac, " +
      "open_time, sold_time,login_time,wifi_mac ,current_ip" +
      s" from mtv_terminal where status =1 and activate_status =1 " +
      s"and serial_number not like 'XX%' " +
      s"and (id !=1339211 and id != 10036711) and serial_number is not null " +
      s"and serial_number <> ''").registerTempTable("mtv_terminal_info")


    //从长连接中找出各SN号对应的IP

    val aliveInfo = MysqlDB.lcmsAccount
    sqlContext.read.format("jdbc").options(aliveInfo).load().where("product = 'whaley'").persist(StorageLevel.MEMORY_AND_DISK).registerTempTable("account")

    //选出SN和对应的最后的时间，保证每个SN只对应一个时间
    sqlContext.sql("select sn ,max(last_time) AS last_time from account where product = 'whaley'  group by sn ").registerTempTable("sn_time")

    //根据SN和时间选择出对应的IP，只限制最新更新的IP
    sqlContext.sql("select a.sn,b.real_client_ip from  sn_time a left join account b on a.last_time = b.last_time and a.sn = b.sn where b.product = 'whaley'").registerTempTable("account_info")

    //将用户全量表中的IP和长连接中的IP进行比较如果某个SN在长连接和全量表中均存在则以长连接为准，否则以全量表为准
    sqlContext.sql("select a.serial_number as serial_number,a.service_id as service_id, a.rom_version as rom_version," +
      "a.wui_version as wui_version,a.mac as mac,a.open_time as open_time,a.sold_time as sold_time, a.login_time as login_time, a.wifi_mac as wifi_mac," +
      " case when b.real_client_ip is null then a.current_ip  when trim(real_client_ip) ='' then a.current_ip  else b.real_client_ip end as ip" +
      " from mtv_terminal_info a left join account_info b on a.serial_number = b.sn").registerTempTable("user_info")


    //获取用户机型信息
    val productModulInfo = MysqlDB.dwDimensionDb("whaley_sn_to_productmodel")
    sqlContext.read.format("jdbc").options(productModulInfo).load().registerTempTable("whaley_sn_to_product_model")
    sqlContext.sql("select serial_number , product_model  from whaley_sn_to_product_model ").registerTempTable("product_model")

    sqlContext.udf.register("snToProductLine", snToProductLine _)
    sqlContext.udf.register("getSNtwo", getSNtwo _)

    //将用户机型信息加入用户信息中存为中间表“product”
    sqlContext.sql("select a.serial_number as product_sn, snToProductLine(a.serial_number) as product_line ," +
      "case when b.product_model is null then '未知' else b.product_model end as product_model," +
      " a.service_id as user_id, a.rom_version as rom_version,a.wui_version as wui_version,a.mac as mac,a.open_time as open_time, " +
      "a.sold_time as sold_time,a.login_time as login_time,a.wifi_mac as wifi_mac, " +
      "a.ip as ip from user_info a left join product_model b on getSNtwo(a.serial_number) = b.serial_number").registerTempTable("product")


    //获取用户的会员信息(该会员信息有误，废弃）
    val vipTypeInfo = MysqlDB.whaleyDolphin("dolphin_club_authority", "id", 1, 100000000, 100)

    sqlContext.read.format("jdbc").options(vipTypeInfo).load().registerTempTable("dolphin_club_authority")
    sqlContext.sql("select distinct sn , 'vip' as  vip  from dolphin_club_authority where sn not like 'XX%'").registerTempTable("vipType")


    //将用户会员信息加入用户信息中作为中间表。表明为"userVipInfo"
    sqlContext.sql("select a.product_sn as product_sn, a.product_line as product_line ,a.product_model as product_model," +
      " a.user_id as user_id, a.rom_version as rom_version,a.wui_version as wui_version,a.mac as mac,a.open_time as open_time," +
      "a.sold_time as sold_time, a.login_time as login_time,a.wifi_mac as wifi_mac," +
      " a.ip as ip, case b.vip when 'vip' then 'vip' else '未知' end  as vip_type from product a left join vipType b on a.product_sn = b.sn")
      .registerTempTable("userVipInfo")

    //获取用户的城市信息

    sqlContext.udf.register("getFirstIpInfo", getFirstIpInfo _)
    sqlContext.udf.register("getSecondIpInfo", getSecondIpInfo _)
    sqlContext.udf.register("getThirdIpInfo", getThirdIpInfo _)

    sqlContext.read.parquet("/data_warehouse/dw_dimensions/dim_web_location").registerTempTable("log_data")
    sqlContext.sql("select ip_section_1,ip_section_2,ip_section_3,country,area,province,city,district,isp,city_level,prefecture_level_city,dim_invalid_time from log_data").registerTempTable("countryInfo")

    sqlContext.sql("select a.product_sn as product_sn, a.product_line as product_line ,a.product_model as product_model," +
      " a.user_id as user_id, a.rom_version as rom_version,a.wui_version as wui_version,a.mac as mac,a.open_time as open_time," +
      "a.sold_time as sold_time,a.login_time as login_time, a.wifi_mac as wifi_mac,a.ip as ip," +
      "a.vip_type  as vip_type ,case when b.country = '' then '未知' when  b.country is null  then '未知' else b.country end  as country," +
      "case when b.area = '' then '未知' when b.area is null then '未知' else b.area end  as area," +
      "case when b.province = '' then '未知' when b.province is null then '未知' else b.province end as province," +
      "case when b.city = '' then '未知' when b.city is null then '未知' else b.city end as city," +
      "case when b.district = '' then '未知' when b.district is null then '未知' else b.district end as district," +
      "case when b.isp = '' then '未知' when b.isp is null then '未知' else b.isp end as isp," +
      "case when b.city_level = '' then '未知' when b.city_level is null then '未知' else b.city_level end as city_level ," +
      "case when b.prefecture_level_city = '' then '未知' when b.prefecture_level_city is null then '未知' else b.prefecture_level_city end as prefecture_level_city" +
      " from userVipInfo a left join countryInfo b on getFirstIpInfo(a.ip) = b.ip_section_1" +
      " and getSecondIpInfo(a.ip) = b.ip_section_2 and getThirdIpInfo(a.ip) = b.ip_section_3 and b.dim_invalid_time is null")


  }

  def snToProductLine(productSN:String) = {
    if(productSN == null || productSN == ""){
      "helios"
    } else if(productSN.startsWith("P") || productSN.startsWith("A") || productSN.startsWith("B") ||productSN.startsWith("C")){
      "orca"
    }else "helios"
  }


  def getSNtwo(sn: String) = {
    if (sn != null) {
      if (sn.length() >= 10) {
        val snPrefix = sn.substring(0, 2)
        snPrefix
      } else null
    } else null
  }

  def getFirstIpInfo(ip: String) = {
    var firstip = -1
    if (ip != null) {
      val ipinfo = ip.split("\\.")
      if (ipinfo.length >= 3) {
        firstip = ipinfo(0).trim().toInt
      }
    }
    firstip
  }

  def getSecondIpInfo(ip: String) = {
    var secondip = -1
    if (ip != null) {
      val ipinfo = ip.split("\\.")
      if (ipinfo.length >= 3) {
        secondip = ipinfo(1).trim().toInt
      }
    }
    secondip
  }

  def getThirdIpInfo(ip: String) = {
    var thirdip = -1
    if (ip != null) {
      val ipinfo = ip.split("\\.")
      if (ipinfo.length >= 3) {
        thirdip = ipinfo(2).trim().toInt
      }
    }
    thirdip
  }

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

}
