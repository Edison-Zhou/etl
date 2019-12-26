package cn.whaley.datawarehouse.normalized.medusa

import java.io.File
import java.util.{Calendar, Date}
import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.dimension.constant.Constants.{NORMALIZED_TABLE_HDFS_BASE_PATH_BACKUP, NORMALIZED_TABLE_HDFS_BASE_PATH_DELETE, NORMALIZED_TABLE_HDFS_BASE_PATH_TMP, THRESHOLD_VALUE}
import cn.whaley.datawarehouse.fact.constant.LogPath
import cn.whaley.datawarehouse.global.FilterType
import cn.whaley.datawarehouse.global.Globals.NORMALIZED_TABLE_HDFS_BASE_PATH
import cn.whaley.datawarehouse.util.{DataExtractUtils, DateFormatUtils, HdfsUtil, Params}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable.ListBuffer
import cn.whaley.datawarehouse.global.Constants._


/**
  * Created by xia.jun on 2017/11/7.
  * 该类用于绑定订单购买入口与购买时所在设备的UID，未找到购买入口的订单踢出
  */
object BindUidEntranceToOrder extends BaseClass{


  /*****************************************************************
    * Phase 1 extract
    * @param params
    *
    */
  override def extract(params: Params) = {

    val refreshable = params.isRefresh
    params.paramMap.get("date") match {
      case Some(p) => {
        /** 维度表数据*/
        val dimAccountDF = DataExtractUtils.readFromParquet(sqlContext, LogPath.MEDUSA_ACCOUNT).filter("dim_invalid_time is null")
        val dimGoodDF = DataExtractUtils.readFromParquet(sqlContext, LogPath.DIM_MEDUSA_MEMBER_GOOD).filter("dim_invalid_time is null and is_valid = 1")
        val calendar = Calendar.getInstance()
        calendar.setTime(DateFormatUtils.readFormat.parse(p.toString))
        val cnFormatDay = DateFormatUtils.cnFormat.format(calendar.getTime)

        /** 订单事实表数据*/
        val todayOrderDF = sqlContext.sql(
          s"""
            |select * from $DW_FACTS.$FACT_MEDUSA_MEMBER_ORDER
             where day_p = '${p.toString}'
          """.stripMargin)


        /** 入口日志与登录账户日志*/
        val entranceDF = DataExtractUtils.readFromOds(sqlContext, "ods_view.log_medusa_main3x_medusa_vipentrance_click", p.toString, null).
          select("userId", "accountId", "entrance", "videoSid","recommendType", "date", "datetime", "happenTime","alg","biz","path")
        val accountLoginDF = DataExtractUtils.readFromOds(sqlContext, "ods_view.log_medusa_main3x_mtvaccount", p.toString, null)
          .select("userId", "accountId", "date")
          .union(DataExtractUtils.readFromOds(sqlContext, "ods_view.log_medusa_main3x_homeview", p.toString, null).
            select("userId", "accountId", "date"))
          .union(DataExtractUtils.readFromOds(sqlContext, "ods_view.log_medusa_main3x_homeaccess", p.toString, null).
            select("userId", "accountId", "date"))
          .distinct()
        val bindAccount2EntranceDF = bindAccountInfo(accountLoginDF, entranceDF)
        //        val bindGoodOrderDF = todayOrderDF.join(dimGoodDF, todayOrderDF("good_sk") === dimGoodDF("good_sk")).drop(dimGoodDF("good_sk"))
        //        val finalOrderDF2 = bindGoodOrderDF.join(dimAccountDF, bindGoodOrderDF("account_sk") === dimAccountDF("account_sk")).drop(dimAccountDF("account_sk"))
        val finalOrderDF = todayOrderDF.join(dimAccountDF, todayOrderDF("account_sk") === dimAccountDF("account_sk")).drop(dimAccountDF("account_sk"))
        val todayMappedOrder = bindEntrance(finalOrderDF, bindAccount2EntranceDF)
        val todayMappedOrderCode = todayMappedOrder.select("order_code")
        val todayUnMappedOrder = finalOrderDF.except(finalOrderDF.join(todayMappedOrderCode,Seq("order_code")))
        //将自动续费的订单的entrance置为连续包月
        val todayContinuousMonthOrder = todayUnMappedOrder.filter("is_auto_renewal = '1'")
          .withColumn("entrance", lit("continuousMonth"))
          .withColumn("video_sid", lit(""))
          .withColumn("recommend_type", lit(""))
          .withColumn("alg", lit(""))
          .withColumn("biz", lit(""))
          .withColumn("path", lit("")) //连续包月订单的video_sid、recommend_type、alg、biz、path都没有
          .select("order_code", "account_id", "dim_date", "user_id", "entrance", "video_sid", "recommend_type", "alg", "biz", "path")
        val mappedDF = if (HdfsUtil.pathIsExist(LogPath.ORDER_ENTRANCE_UID_MAPPED)) {
          val oldDF = DataExtractUtils.readFromParquet(sqlContext, LogPath.ORDER_ENTRANCE_UID_MAPPED)
          // @date 2018-03-01
          // @description 如果需要添加新的字段的时候，需要恢复历史数据的时候，避免要从头开始计算
          val previousMappedOrder = if(refreshable){
            oldDF.filter(s"dim_date < '${cnFormatDay}'")
          }else{
            oldDF
          }.select("order_code", "account_id", "dim_date", "user_id", "entrance", "video_sid", "recommend_type", "alg", "biz", "path")

          //          val finalPreviousMappedOrder = previousMappedOrder.join(dimGoodDF,Seq("good_sk"))
          //val continuousMonthOrder = bindContinuousMonthOrder(todayUnMappedOrder, finalPreviousMappedOrder)
          todayContinuousMonthOrder.printSchema()
          todayMappedOrder.printSchema()
          previousMappedOrder.printSchema()

          todayContinuousMonthOrder.union(todayMappedOrder).union(previousMappedOrder).distinct()
        }else {
          todayMappedOrder.distinct()
        }

        // 修正入口数据
        mappedDF

      }
      case None => throw new RuntimeException("未设置时间参数！")
    }
  }


  /**
    * 将sourceDF中的account_id信息绑定到destinationDF中
    * @param sourceDF
    * @param destinationDF
    */
  def bindAccountInfo(sourceDF:DataFrame, destinationDF:DataFrame) = {

    val newSourceDF = sourceDF.withColumnRenamed("accountId","sourceAccount")
    val newDestinationDF = destinationDF.withColumnRenamed("accountId","destinationAccount")

    newDestinationDF.join(newSourceDF, newSourceDF("userId") === newDestinationDF("userId") && newSourceDF("date") === newDestinationDF("date"), "left_outer").
      withColumn("accountId", expr("case when destinationAccount > 0 then destinationAccount else sourceAccount end")).
      select(newDestinationDF("userId"), newDestinationDF("date"), newDestinationDF("datetime"),newDestinationDF("happenTime"), newDestinationDF("entrance"),
        newDestinationDF("videoSid"), newDestinationDF("recommendType"), newDestinationDF("alg"), newDestinationDF("biz"), newDestinationDF("path"), col("accountId")).distinct()
  }

  /**
    * 提取日志路径中的日期信息
    * @param startDate
    * @param endDate
    * @return
    */
  def getPathDate(startDate:Date, endDate:Date):String = {
    var dateArr = ListBuffer[String]()
    val dateDiffs = (endDate.getTime - startDate.getTime) / (1000*3600*24)
    val calendar = Calendar.getInstance()
    calendar.setTime(startDate)
    (0 to dateDiffs.toInt).foreach(i => {
      dateArr.+=(DateFormatUtils.readFormat.format(calendar.getTime))
      calendar.add(Calendar.DAY_OF_MONTH, 1)
    })
    "{" + dateArr.mkString(",") + "}"
  }

  /**
    * 绑定购买入口信息
    * @param orderDF
    * @param entranceDF
    */
  def bindEntrance(orderDF:DataFrame, entranceDF:DataFrame):DataFrame = {

    val allEntranceByAccountIdDF = orderDF.join(entranceDF,
      orderDF("account_id") === entranceDF("accountId"))

    val orderEntranceByAccountDF = allEntranceByAccountIdDF.filter(unix_timestamp(col("create_time")) * 1000 > col("happenTime"))
      .groupBy("order_code", "account_id", "dim_date", "good_sk").agg("happenTime" -> "max")

    val originDf = orderEntranceByAccountDF.join(entranceDF,
      orderEntranceByAccountDF("account_id") === entranceDF("accountId") &&
        orderEntranceByAccountDF("max(happenTime)") === entranceDF("happenTime")).
      select("order_code","account_id", "dim_date","good_sk","userId","entrance","videoSid","recommendType","alg","biz","path").
      withColumnRenamed("videoSid","video_sid").withColumnRenamed("recommendType","recommend_type")
    val df = originDf.withColumn("condition_column",concat_ws(",",originDf("userId"),originDf("entrance"),
      originDf("video_sid"),originDf("recommend_type"),originDf("alg"),originDf("biz"),originDf("path")))

    // 用来处理日志重复的问题
    val filterDf = df.groupBy("order_code","account_id", "dim_date","good_sk").agg("condition_column" -> "max")

    val result = df.join(filterDf, filterDf("max(condition_column)") === df("condition_column")).
      select(df("order_code"), df("account_id"), df("dim_date")
        //        , df("good_sk")
        , df("userId"), df("entrance"), df("video_sid"), df("recommend_type"), df("alg"), df("biz"), df("path")).withColumnRenamed("userId", "user_id").distinct
    result
  }


  /**
    * 将订单匹配前一天已经匹配入口的订单，匹配不到的为null，未做删除
    *
    * @param originOrderDF 原始订单
    * @param mappedOrderDF 已经匹配订单
    */
  def bindContinuousMonthOrder(originOrderDF:DataFrame, mappedOrderDF:DataFrame) = {
    // 选择出连续包月的订单
    val consecutiveAccountDF = originOrderDF.filter(s"good_name = '${FilterType.CONSECUTIVE_MONTH_ORDER}'").select("account_id", "order_code", "dim_date","good_sk")
    val accountLastOrderDate = mappedOrderDF.filter(s"good_name = '${FilterType.CONSECUTIVE_MONTH_ORDER}'").select("account_id","dim_date").groupBy("account_id").agg(max("dim_date"))
    val consecutiveMappedDF = mappedOrderDF.filter(s"good_name = '${FilterType.CONSECUTIVE_MONTH_ORDER}'")
    val origrinDf = consecutiveMappedDF.join(accountLastOrderDate, consecutiveMappedDF("account_id")===accountLastOrderDate("account_id") &&
      consecutiveMappedDF("dim_date")===accountLastOrderDate("max(dim_date)")).
      select(consecutiveMappedDF("account_id"),consecutiveMappedDF("user_id"),consecutiveMappedDF("entrance"),
        consecutiveMappedDF("video_sid"),consecutiveMappedDF("recommend_type"),consecutiveMappedDF("alg"),consecutiveMappedDF("biz"),consecutiveMappedDF("path"))
    val df = origrinDf.withColumn("condition_column",concat_ws(",",origrinDf("user_id"),origrinDf("entrance"),origrinDf("video_sid"),
      origrinDf("recommend_type"),origrinDf("alg"),origrinDf("biz"),origrinDf("path")))

    // 用来处理日志重复的问题
    val filterDf = df.groupBy("account_id").agg("condition_column"->"max")
    val referMappedDF = df.join(filterDf, filterDf("max(condition_column)") === df("condition_column")).
      select(df("account_id"),df("user_id"),df("entrance"),df("video_sid"),df("recommend_type"),df("alg"),df("biz"),df("path"))
    val result = consecutiveAccountDF.join(referMappedDF, Seq("account_id")).drop(referMappedDF("account_id")).
      select("order_code","account_id","dim_date","good_sk", "user_id","entrance","video_sid","recommend_type","alg","biz","path")

    result
  }

  /** 时间戳转时间字符串*/
  val timestamp2TimeStr = udf((timeStamp: Long) => {
    DateFormatUtils.detailFormat.format(new Date(timeStamp))
  })




  /*****************************************************************
    * Phase 2 transform
    * @param params
    * @param df
    *
    */
  override def transform(params: Params, df: DataFrame) = {
    df
  }



  /*****************************************************************
    * Phase 3 load
    * @param params
    * @param df
    *
    */
  override def load(params: Params, df: DataFrame):Unit = {
    backup(params,df,"medusa_order_uid_entrance_mapping")
  }

  /**
    * 用来备份维度数据，然后将维度数据生成在临时目录，当isOnline参数为true的时候，将临时目录的数据替换线上维度
    *
    * @param p  the main args
    * @param df the DataFrame from execute function
    * @return a Unit.
    */
  private def backup(p: Params, df: DataFrame, dimensionType: String): Unit = {

    val cal = Calendar.getInstance
    val date = DateFormatUtils.readFormat.format(cal.getTime)
    val onLineNormizedDir = NORMALIZED_TABLE_HDFS_BASE_PATH + File.separator + dimensionType
    val onLineNormizedBackupDir = NORMALIZED_TABLE_HDFS_BASE_PATH_BACKUP + File.separator + date + File.separator + dimensionType
    val onLineNormizedDirTmp = NORMALIZED_TABLE_HDFS_BASE_PATH_TMP + File.separator + dimensionType
    val onLineNormizedDirDelete = NORMALIZED_TABLE_HDFS_BASE_PATH_DELETE + File.separator + dimensionType
    println("线上数据目录:" + onLineNormizedBackupDir)
    println("线上数据备份目录:" + onLineNormizedBackupDir)
    println("线上数据临时目录:" + onLineNormizedDirTmp)
    println("线上数据等待删除目录:" + onLineNormizedDirDelete)

    df.persist(StorageLevel.MEMORY_AND_DISK)
    val isOnlineFileExist = HdfsUtil.IsDirExist(onLineNormizedDir)
    if (isOnlineFileExist) {
      val isBackupExist = HdfsUtil.IsDirExist(onLineNormizedBackupDir)
      if (isBackupExist) {
        println("数据已经备份,跳过备份过程")
      } else {
        println("生成线上维度备份数据:" + onLineNormizedBackupDir)
        val isSuccessBackup = HdfsUtil.copyFilesInDir(onLineNormizedDir, onLineNormizedBackupDir)
        println("备份数据状态:" + isSuccessBackup)
      }
    } else {
      println("无可用备份数据")
    }

    //防止文件碎片
    val total_count = BigDecimal(df.count())
    val partition = Math.max(1, (total_count / THRESHOLD_VALUE).intValue())
    println("repartition:" + partition)

    val isTmpExist = HdfsUtil.IsDirExist(onLineNormizedDirTmp)
    if (isTmpExist) {
      println("删除线上维度临时数据:" + onLineNormizedDirTmp)
      HdfsUtil.deleteHDFSFileOrPath(onLineNormizedDirTmp)
    }
    println("生成线上维度数据到临时目录:" + onLineNormizedDirTmp)
    df.repartition(partition).write.parquet(onLineNormizedDirTmp)

    println("数据是否上线:" + p.isOnline)
    if (p.isOnline) {
      println("数据上线:" + onLineNormizedDir)
      if (isOnlineFileExist) {
        println("移动线上维度数据:from " + onLineNormizedDir + " to " + onLineNormizedDirDelete)
        val isRenameSuccess = HdfsUtil.rename(onLineNormizedDir, onLineNormizedDirDelete)
        println("isRenameSuccess:" + isRenameSuccess)
      }

      val isOnlineFileExistAfterRename = HdfsUtil.IsDirExist(onLineNormizedDir)
      if (isOnlineFileExistAfterRename) {
        throw new RuntimeException("rename failed")
      } else {
        val isSuccess = HdfsUtil.rename(onLineNormizedDirTmp, onLineNormizedDir)
        println("数据上线状态:" + isSuccess)
      }
      println("删除过期数据:" + onLineNormizedDirDelete)
      HdfsUtil.deleteHDFSFileOrPath(onLineNormizedDirDelete)
    }
  }




}
