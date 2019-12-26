package cn.whaley.datawarehouse.fact.moretv

import java.io.File
import java.util.{Calendar, Date}

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition, UserDefinedColumn}
import cn.whaley.datawarehouse.fact.FactEtlBase
import cn.whaley.datawarehouse.fact.constant.Constants.FACT_HDFS_BASE_PATH_TMP
import cn.whaley.datawarehouse.fact.constant.LogPath
import cn.whaley.datawarehouse.global.FilterType
import cn.whaley.datawarehouse.util._
import cn.whaley.datawarehouse.util.DesUtil._
import org.apache.avro.TestAnnotation
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit, min, udf}

import scala.collection.mutable.ListBuffer

/**
  * Created by xia.jun on 2017/11/9.
  */
object MemberOrder extends FactEtlBase {

  topicName = "fact_medusa_member_order"

  source = "default"

  columnsFromSource = List(
    ("order_code", "order_code"),
    ("order_desc", "order_desc"),
    ("vip_pay_time", "vip_pay_time"),
    ("vip_start_time", "vip_start_time"),
    ("vip_end_time", "vip_end_time"),
    ("activity_id", "activity_id"),
    ("total_price", "total_price"),
    ("real_price", "real_price"),
    ("payment_amount", "payment_amount"),
    ("business_type", "business_type"),
    ("pay_channel", "pay_channel"),
    ("order_channel", "order_channel"),
    ("order_type", "order_type"),
    ("valid_status", "valid_status"),
    ("trade_status", "trade_status"),
    ("pay_status", "pay_status"),
    ("duration", "duration"),
    ("cid", "cid"),
    ("create_time", "create_time"),
    ("update_time", "update_time"),
    ("dim_date", "dim_date"),
    ("dim_time", "dim_time"),
    ("user_id", "user_id"),
    ("promotion_channel", "promotion_channel"),
    ("is_auto_renewal", "is_auto_renewal"),
    ("alg", "alg"),
    ("biz", "biz"),
    ("order_source", "order_source")
  )

  override def readSource(sourceDate: String, sourceHour: String): DataFrame = {
    val sourceDB = MysqlDB.medusaMemberDB("business_order")
    val businessOrderDF = DataExtractUtils.readFromJdbc(sqlContext, sourceDB)
    var df = businessOrderDF.filter(s"substring(vip_pay_time,0,10) = '${DateFormatUtils.cnFormat.format(DateFormatUtils.readFormat.parse(sourceDate))}'")
    if (sourceHour != null) {
      df = df.filter(s"substring(vip_pay_time,12,2) = '$sourceHour'")
    }


    //408版本及之后order_source里面传的是8位的key，与购买入口点击日志里面的secret_key一致
    df.filter("length(order_source) != 8 or order_source is null").createTempView("no_encode")

    spark.udf.register("parse_mtvip_order_source", parseMtvipOrderSource: (String, String) => String)

    //没有加密的key也就是408版本之前的订单，需要将order_source解密后解析user_id和promotion_channel
    val noEncodeDf = sqlContext.sql(
      """
        |select *
        |,parse_mtvip_order_source(order_source,'user_id') as user_id
        |,parse_mtvip_order_source(order_source,'promotion_channel') as promotion_channel
        |,null as alg,null as biz
        |from no_encode
      """.stripMargin)

    //购买入口数据
    DataExtractUtils.readFromOds(sqlContext, "ods_view.log_medusa_main4x_vipentrance_click", sourceDate, sourceHour).createTempView("entrance")

    //408版本及之后order_source里面传的是8位的key，与购买入口点击日志里面的secret_key一致
    df.filter("length(order_source) = 8").createTempView("encode")

    //account_id是否有值分开join
    val encodeDf = sqlContext.sql(
      """
        |select a.*,b.user_id,b.promotion_channel,b.alg,b.biz
        |from encode a join entrance b
        |on a.account_id = b.account_id and a.order_source = b.secret_key and a.sid = b.sid
        |union
        |select a.*,b.user_id,b.promotion_channel,b.alg,b.biz
        |from encode a left join entrance b
        |on a.account_id = b.account_id and a.order_source = b.secret_key
        |where a.sid is null
      """.stripMargin)

    noEncodeDf.union(encodeDf)
  }

  //  override def load(params: Params, df: DataFrame) = {
  //    val goodsDF = DataExtractUtils.readFromParquet(sqlContext, LogPath.DIM_MEDUSA_MEMBER_GOOD).filter("dim_invalid_time is null and is_valid = 1").select("good_sk", "good_name").withColumnRenamed("good_sk", "dim_good_sk")
  ////    val finalDf = addContMonOrderFlag(params,df,goodsDF)
  //    super.load(params,goodsDF)
  //  }

  //  def addContMonOrderFlag(params: Params,df:DataFrame,goodsDF:DataFrame):DataFrame = {
  //    params.paramMap.get("date") match {
  //      case Some(p) => {
  //        if(!p.toString.equals("20170811")) {
  //
  //         val startDate = "20170811"
  //         val endDate = p.toString
  //         val previousOrderDF = sqlContext.sql(
  //           s"""
  //             |select * from dw_facts.fact_medusa_member_order
  //             |where day_p>='$startDate' and day_p<='$endDate'
  //           """.stripMargin)
  //          val previousConMonOrderDF = previousOrderDF.join(goodsDF,previousOrderDF("good_sk")===goodsDF("dim_good_sk")).
  //            filter(s"good_name = '${FilterType.CONSECUTIVE_MONTH_ORDER}'").select("account_sk","vip_end_time")
  //          val mergerDF = df.join(goodsDF,df("good_sk") === goodsDF("dim_good_sk"))
  //
  //          /** Case One: 非连续包月订单*/
  //          val todayNonConMonOrderDF = mergerDF.filter(s"good_name != '${FilterType.CONSECUTIVE_MONTH_ORDER}'").
  //            drop("dim_good_sk").drop("good_name").withColumn("is_first_cont_mon_order", lit(0))
  //
  //          /** Case Two: 连续包月订单*/
  //          val todayConMonOrderDF = mergerDF.filter(s"good_name = '${FilterType.CONSECUTIVE_MONTH_ORDER}'")
  //            .drop("dim_good_sk").drop("good_name")
  //
  //          // 获取当日订单中vip_start_time最早的一笔订单（这种情况是用来处理一天内出现多笔连续包月的订单情况）
  //          val todayAccountFirstVipStartDateDF  = todayConMonOrderDF.groupBy("account_sk").agg(min(("vip_start_time"))).withColumnRenamed("account_sk","account_sk_first")
  //          val todayFirstVipConMonOrderDF = todayConMonOrderDF.join(todayAccountFirstVipStartDateDF,
  //            todayConMonOrderDF("account_sk") === todayAccountFirstVipStartDateDF("account_sk_first") &&
  //              todayConMonOrderDF("vip_start_time") === todayAccountFirstVipStartDateDF("min(vip_start_time)")).
  //            drop(todayAccountFirstVipStartDateDF("account_sk_first")).drop(todayAccountFirstVipStartDateDF("min(vip_start_time)"))
  //
  //          val todayNonFirstVipConMonOrderDF = todayConMonOrderDF.except(todayFirstVipConMonOrderDF).
  //            withColumn("is_first_cont_mon_order", lit(0))
  //
  //          // 处理当日内的订单是否是属于之前连续包月订单中的一部分
  //          val nonFirstConMonOrderDF = todayFirstVipConMonOrderDF.join(previousConMonOrderDF,
  //            todayFirstVipConMonOrderDF("account_sk") === previousConMonOrderDF("account_sk") &&
  //              getDate(todayFirstVipConMonOrderDF("vip_start_time"))=== getDate(previousConMonOrderDF("vip_end_time"))).
  //            select(todayFirstVipConMonOrderDF("order_code"))
  //          val nonFirstConMonDF = todayFirstVipConMonOrderDF.join(nonFirstConMonOrderDF,Seq("order_code"))
  //
  //          val firstConMonDF = todayFirstVipConMonOrderDF.except(nonFirstConMonDF).withColumn("is_first_cont_mon_order", lit(1))
  //
  //          todayNonConMonOrderDF.union(todayNonFirstVipConMonOrderDF).union(nonFirstConMonDF.withColumn("is_first_cont_mon_order",lit(0))).union(firstConMonDF)
  //
  //        } else {
  //          df.withColumn("is_first_cont_mon_order",lit(0))
  //        }
  //      }
  //      case None => {
  //        throw new RuntimeException("未设置时间参数！")
  //      }
  //    }
  //
  //  }

  addColumns = List(
    UserDefinedColumn("dim_date", udf(getDimDate: String => String), List("create_time")),
    UserDefinedColumn("dim_time", udf(getDimTime: String => String), List("create_time"))
    //    UserDefinedColumn("user_id", udf { str: String =>
    //      if (str != "" && str != null) { //order_source解密后是(user_id + "," + promotion_channel)的字符串，order_source从408版本开始不传user_id和promotion_channel的加密字符串
    //        val decodeStr = decrypt(str).split(",")
    //        if (decodeStr.length > 0
    //          && decodeStr(0).length >= 32 //order_source如果是408之后传上来的就是一个八位的key，要排除这种情况
    //        ) {
    //          decodeStr(0)
    //        } else null
    //      } else null
    //    }, List("order_source")),
    //    UserDefinedColumn("promotion_channel", udf { str: String =>
    //      if (str != "" && str != null) {
    //        val decodeStr = decrypt(str).split(",")
    //        if (decodeStr.length > 1) {
    //          decodeStr(1)
    //        } else null
    //      } else null
    //    }, List("order_source"))
  )


  dimensionColumns = List(

    //废弃good_sk，原因是此维度表是由会员的同事手动维护的，主要用的就是good_name,可以通过订单明细表中的价格来区分商品名称
    /** 基于订单中的real_price获取对应的商品维度good_sk */
    //    new DimensionColumn("dim_medusa_member_goods",
    //      List(DimensionJoinCondition(Map("payment_amount" -> "good_price", "member_code" -> "member_code"))),
    //      "good_sk","good_sk"),

    /** 基于订单中的account_id获取账号表中的账号维度account_sk */
    new DimensionColumn("dim_medusa_account",
      List(DimensionJoinCondition(Map("account_id" -> "account_id"))),
      "account_sk", "account_sk")
  )

  val getDate = udf((time: String) => {
    time.substring(0, 10)
  })


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

  /**
    * 提取日志路径中的日期信息
    * @param startDate
    * @param endDate
    * @return
    */
  def getPathDate(startDate: Date, endDate: Date): String = {
    var dateArr = ListBuffer[String]()
    val dateDiffs = (endDate.getTime - startDate.getTime) / (1000 * 3600 * 24)
    val calendar = Calendar.getInstance()
    calendar.setTime(startDate)
    (0 to dateDiffs.toInt).foreach(i => {
      dateArr.+=(DateFormatUtils.readFormat.format(calendar.getTime))
      calendar.add(Calendar.DAY_OF_MONTH, 1)
    })
    "{" + dateArr.mkString(",") + "}"
  }

  /**
    * 解析影视VIP订单408版本之前传过来的order_source字段，里面解密后是(user_id + "," + promotion_channel)的字符串
    *
    * @param order_source 加密字段
    * @param column       需要传回来的字段，可选值user_id和promotion_channel
    * @return
    */
  def parseMtvipOrderSource(order_source: String, column: String): String = {

    val decodeStr = decrypt(order_source).split(",")

    if (column == "user_id" && decodeStr.length > 0) decodeStr(0)
    else if (column == "promotion_channel" && decodeStr.length > 1) decodeStr(1)
    else null
  }
  //  @TestAnnotation
  //  override def main(args: Array[String]): Unit = {
  //    val str = "30dc58f2f09a51bf9d3f8d0a999ef5e7dc24088d22cf3f578a6b8ee7dfd75a0c321d8b0213ea410fd2e43110a33f615b"
  //    val str1 = ""
  //    val str2 = null
  //    println(str1 != null)
  //    println(str2 == null)
  //    //println(DesUtil.decrypt(str1))
  //    println(str1.toString.length)
  //println(str1.split(",").length)
  //    //println(DesUtil.decrypt(str1).split(",")(0))
  //    //println(DesUtil.decrypt(str1).split(",")(1))
  //  }


}
