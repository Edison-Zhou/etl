package cn.whaley.datawarehouse.market.event

import java.io.File
import java.util.Date

import cn.moretv.bigdata.hive.HiveSdk
import cn.moretv.bigdata.hive.global.EnvEnum
import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.common.{DimensionColumn, UserDefinedColumn}
import cn.whaley.datawarehouse.global.Globals._
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.market.event.constant.Constants._
import cn.whaley.datawarehouse.util._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.storage.StorageLevel

/**
  * @author wangning
  * @date 2019/9/5 15:41
  */
abstract class MarketEtlBase extends BaseClass {

  /**
    * 索引
    */
  val INDEX_NAME = "source_index"
  /**
    * 大宽表中需要的字段
    */
  var columnsFromSource: List[(String, String)] = Nil
  /**
    * 大宽表表名
    */
  var topicName: String = _
  /**
    * 来源分区
    */
  var source: String = _
  /**
    * parquet路径
    */
  var parquetPath: String = _
  /**
    * 读取数据源表名，默认是读取dw_facts库下的表
    */
  var factTableName: String = _
  /**
    * 添加列
    */
  var addColumns: List[UserDefinedColumn] = Nil
  /**
    * 维度配置
    */
  var dimensionColumns: List[DimensionColumn] = Nil

  /**
    * 在最终获取大宽表字段时需要用到的维度表名称
    */
  var dimensionsNeedInFact: List[String] = Nil

  var partition: Int = 0

  /**
    * 日志唯一id列字段名。
    * 如果设置了该参数，需要保证每一条日志的该列值各不相同，并且不能为空。
    * 该参数如果为空，则默认使用logid
    * 如果设定的字段不存在，则系统会自动生成唯一字段
    */
  var uniqueLogIdColumn: String = null

  /**
    * 事实发生的时间，格式yyyy-MM-dd HH:mm:ss
    */
  var factTime: String = "concat(dim_date, '', dim_time)"

  var dfLineCount = 0L


  /**
    * 源数据读取函数, ETL中的Extract
    * 如需自定义，可以在子类中重载实现
    *
    * @return
    */
  override def extract(params: Params): DataFrame = {
    params.paramMap.get("date") match {
      case Some(x) => {
        if (partition == 0)
          readSource(x.toString, params.startHour)
        else
          readSource(x.toString, params.startHour).repartition(partition)
      }
      case None =>
        throw new RuntimeException("未设置时间参数。。。")
    }
  }

  /**
    * 获取数据源 默认读取dw_facts库下的数据源，可在子类中重写
    *
    * @param sourceDate
    * @param sourceHour
    * @return
    */
  def readSource(sourceDate: String, sourceHour: String): DataFrame = {
    if (sourceDate == null) {
      null
    }
    else if (readSourceType == null || readSourceType == hive) {
      DataExtractUtils.readFromFact(sqlContext, factTableName, sourceDate, sourceHour)
    }
    else if (readSourceType == hive_parquet) {
      DataExtractUtils.readFromFactParquet(sqlContext, factTableName, source, sourceDate, sourceHour)
    }
    else {
      null
    }
  }

  /**
    * 数据转换函数，ETL中的Transform
    *
    * @return
    */
  override def transform(params: Params, sourceDf: DataFrame): DataFrame = {
    val filteredSourceDf = filterRows(sourceDf)
    val completeSourceDf = addNewColumns(filteredSourceDf)
    if (debug) completeSourceDf.printSchema()

    if (dfLineCount == 0) {
      throw new RuntimeException("未读取到源数据！")
    }

    val completeSourcePath = getPath(params, DM_FACT_HDFS_BASE_PATH_COMPLETE)
    val completeSourcePathExist = HdfsUtil.IsDirExist(completeSourcePath)
    if (completeSourcePathExist) {
      HdfsUtil.deleteHDFSFileOrPath(completeSourcePath)
    }
    completeSourceDf.persist(StorageLevel.MEMORY_AND_DISK_SER)

    //将包括所有字段的DF写入HDFS，用于更新大宽表单个字段
    val load_to_hdfs_partition = repartition(completeSourceDf)
    if (params.isOnline) {
      completeSourceDf.repartition(load_to_hdfs_partition).write.parquet(completeSourcePath)
      println("中间结果数据写入成功。。。")
    }

    if (dimensionColumns.isEmpty) {
      completeSourceDf.selectExpr(columnsFromSource.map(c => c._2 + " as " + c._1) ++ List(uniqueLogIdColumn): _*)
    } else {
      val dimensionJoinDf = parseDimension(completeSourceDf, dimensionColumns, uniqueLogIdColumn, factTime,DM_DIMENSION_HDFS_BASE_PATH)
      val time = DateFormatUtils.compactFormat.format(new Date())
      if (debug) {
        dimensionJoinDf.persist()
        dimensionJoinDf.write.parquet(DM_FACT_HDFS_BASE_PATH + "/debug/" + topicName + "/dimensionJoinDf/" + time)
      }

      //关联源数据和join到的维度
      var df = completeSourceDf.join(dimensionJoinDf, List(uniqueLogIdColumn), "leftouter").as("source")


      // 关联用到的维度
      if (dimensionColumns != null && dimensionsNeedInFact != null) {
        dimensionColumns.foreach(c => {
          if (dimensionsNeedInFact.contains(c.dimensionNameAs)) {
            val dimensionDf = sqlContext.read.parquet(DM_DIMENSION_HDFS_BASE_PATH + File.separator + c.dimensionName)
            df = df.join(dimensionDf.as(c.dimensionNameAs),
              expr("source." + c.factSkColumnName + " = " + c.dimensionNameAs + "." + c.dimensionSkName),
              "leftouter")
          }
        })
      }

      //筛选指定的列
      val result = df.selectExpr(
        columnsFromSource.map(
          c => if (c._2.contains("(") || c._2.contains(" ") || c._2.contains("."))
            c._2 + " as " + c._1
          else
            "source." + c._2 + " as " + c._1)
          ++ dimensionJoinDf.schema.fields.map("source." + _.name)
          : _*
      )
      result
    }

  }

  /**
    * 数据源过滤 可以在子类中进行重写
    * @param sourceDf
    * @return
    */
  def filterRows(sourceDf: DataFrame): DataFrame = {
    sourceDf
  }

  /**
    * 添加新的字段
    *
    * @param sourceDf
    * @return
    */
  private def addNewColumns(sourceDf: DataFrame): DataFrame = {
    var result = sourceDf
    if (addColumns != null) {
      addColumns.foreach(column => {
        if (column.udf != null && column.inputColumns != null) {
          result = result.withColumn(column.name, column.udf(column.inputColumns.map(col): _*))
        } else if (column.expression != null) {
          result = result.withColumn(column.name, expr(column.expression))
        }

      }
      )
    }
    if ((uniqueLogIdColumn == null || uniqueLogIdColumn.trim.isEmpty)
      || !result.schema.fields.exists(p => p.name.equalsIgnoreCase(uniqueLogIdColumn))) {
      //如果设定的唯一键不存在
      println(s"设定的唯一键字段${uniqueLogIdColumn}不存在！！！ 自动生成唯一键字段")
      uniqueLogIdColumn = INDEX_NAME
      result = DataFrameUtil.dfZipWithIndex(result, INDEX_NAME)
      result.persist(StorageLevel.MEMORY_AND_DISK_SER)
      dfLineCount = result.count()
      println("完整大宽表行数：" + dfLineCount)

    } else {
      result.persist(StorageLevel.MEMORY_AND_DISK_SER)
      dfLineCount = result.count()
      println("完整大宽表行数：" + dfLineCount)

      val uniqueColumnDf = result.selectExpr(uniqueLogIdColumn)
      uniqueColumnDf.persist()
      if (uniqueColumnDf.where(s"$uniqueLogIdColumn is null").count() > 0) {
        throw new RuntimeException(s"字段${uniqueLogIdColumn}存在null值，不能设为唯一id字段")
      }

      val distinctCount = uniqueColumnDf.distinct().count()
      println("唯一字段列行数：" + distinctCount)
      if (distinctCount < dfLineCount) {
        throw new RuntimeException(s"字段${uniqueLogIdColumn}存在重复，不能设为唯一id字段")
      }

      uniqueColumnDf.unpersist()

    }

    result
  }

  /**
    * 获取更新单个字段的数据源路径
    *
    * @param params
    * @return completePath
    */
  def getPath(params: Params, path: String): String = {

    val sourceDate = params.paramMap("date")
    val hour = if (params.startHour == null) {
      "00"
    } else {
      params.startHour
    }
    val completePath = path + File.separator + topicName + File.separator + "day_p=" +sourceDate + File.separator + "hour_p=" +hour+ File.separator + "source_p=" +source

    completePath
  }

  /**
    *  重分区
    * @param dataFrame
    * @return
    */
  def repartition(dataFrame: DataFrame): Int = {
    //防止文件碎片
    val lineCount = dataFrame.count
    val total_count = BigDecimal(lineCount)
    val load_to_hdfs_partition = (total_count / DM_THRESHOLD_VALUE).intValue() + 1
    load_to_hdfs_partition
  }

  /**
    * 数据存储函数，ETL中的Load
    */
  override def load(params: Params, df: DataFrame): Unit = {
    backup(params,df,topicName)
  }

  /**
    * 用来备份实时表数据，然后将维度数据生成在临时目录，当isOnline参数为true的时候，将临时目录的数据替换线上维度
    *
    * @param p  the main args
    * @param df the DataFrame from execute function
    * @return a Unit.
    */
  private def backup(p: Params, df: DataFrame, topicName: String): Unit = {
    val date = p.paramMap("date")
    val hour = if (p.startHour == null) {
      "00"
    } else {
      p.startHour
    }

    val partitionParentPath =
      if (source == null || source.trim.isEmpty)
        throw new RuntimeException("source不能为空")
      else
        File.separator + "day_p=" +p.paramMap("date") + File.separator + "hour_p=" +hour
    val partitionPath = partitionParentPath + File.separator +"source_p=" +source

    val onLineFactDir = DM_FACT_HDFS_BASE_PATH + File.separator + topicName + partitionPath
    val onLineFactParentDir = DM_FACT_HDFS_BASE_PATH + File.separator + topicName + partitionParentPath
    val onLineFactBackupDir = DM_HDFS_BASE_PATH_BACKUP + File.separator + topicName + partitionPath
    val onLineFactBackupParentDir = DM_HDFS_BASE_PATH_BACKUP + File.separator + topicName + partitionParentPath
    val onLineFactDirTmp = DM_HDFS_BASE_PATH_TMP + File.separator + topicName + partitionPath
    val sourceDir = DM_FACT_HDFS_BASE_PATH_COMPLETE + File.separator + topicName + partitionPath
    val sourceParentDir = DM_FACT_HDFS_BASE_PATH_COMPLETE + File.separator + topicName + partitionParentPath
    val sourceTempDir = DM_FACT_HDFS_BASE_PATH_COMPLETE_TMP + File.separator + topicName + partitionPath
    println("线上数据目录:" + onLineFactDir)
    println("线上数据备份目录:" + onLineFactBackupDir)
    println("线上数据临时目录:" + onLineFactDirTmp)

    //防止文件碎片
    val total_count = BigDecimal(dfLineCount)
    val load_to_hdfs_partition = (total_count / DM_THRESHOLD_VALUE).intValue() + 1
    println("load_to_hdfs_partition:" + load_to_hdfs_partition)
    //判断临时目录是否存在
    val isTmpExist = HdfsUtil.IsDirExist(onLineFactDirTmp)
    //如果临时目录存在，则删除目录
    if (isTmpExist) {
      HdfsUtil.deleteHDFSFileOrPath(onLineFactDirTmp)
    }
    println("生成线上维度数据到临时目录:" + onLineFactDirTmp)
    //重新分区，然后将维度数据写到临时目录下
    df.repartition(load_to_hdfs_partition).write.parquet(onLineFactDirTmp)
    println("线上维度数据写入临时目录已完成。。。")


    println("数据是否准备上线:" + p.isOnline)
    if (p.isOnline) {
      //判断数据上线目录是否存在
      val isOnlineFileExist = HdfsUtil.IsDirExist(onLineFactDir)
      println("数据上线:" + onLineFactDir)
      if (isOnlineFileExist) {
        println("生成线上维度备份数据:" + onLineFactBackupDir)
        HdfsUtil.deleteHDFSFileOrPath(onLineFactBackupDir)
        HdfsUtil.createDir(onLineFactBackupParentDir)
        val isSuccessBackup = HdfsUtil.rename(onLineFactDir, onLineFactBackupDir)
        println("备份数据状态:" + isSuccessBackup)
      }

      val isOnlineFileExistAfterRename = HdfsUtil.IsDirExist(onLineFactDir)
      if (isOnlineFileExistAfterRename) {
        throw new RuntimeException("rename failed")
      } else {
        val isOnLineFactParentDir = HdfsUtil.createDir(onLineFactParentDir)
        println("数据上线的父目录是否创建成功:" + isOnLineFactParentDir)
        val isSuccess = HdfsUtil.rename(onLineFactDirTmp, onLineFactDir)
        println("数据上线状态:" + isSuccess)
      }

      // add partition
      val partitionMap = Map("day_p" -> date.toString, "hour_p" -> hour)
      val hiveSdk = HiveSdk(EnvEnum.PRODUCT)
      hiveSdk.addPartitionAfterDrop("dw_dm", topicName, partitionMap, onLineFactDir)
    }
    //将临时目录数据移动到线上目录中
    if (!p.factColumn.forall(_.endsWith("_sk")) && !p.factColumn.exists(_ == "default")) {
      val sourceDirExist = HdfsUtil.IsDirExist(sourceDir)
      if (sourceDirExist)
        HdfsUtil.deleteHDFSFileOrPath(sourceDir)
      HdfsUtil.createDir(sourceParentDir)
      val isSuccess = HdfsUtil.rename(sourceTempDir, sourceDir)
      println("中间结果数据移动状态：" + isSuccess)
    }

  }
}
