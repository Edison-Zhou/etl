package cn.whaley.datawarehouse.dimension

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import cn.whaley.datawarehouse.BaseClass
import cn.whaley.datawarehouse.dimension.constant.Constants._
import cn.whaley.datawarehouse.global.Globals._
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util._
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer
import scala.reflect.io.File

/**
  * Created by Tony on 17/3/8.
  */
abstract class DimensionBase extends BaseClass {

  /**
    * 维度表列相关配置
    */
  val columns = new Columns

  /**
    * 来源读取配置jdbc参数，仅当readSourceType设置为jdbc时有效
    */
  var sourceDb: Map[String, String] = _


  /**
    * 维度表名称，同时也是hdfs上的目录名
    */
  var dimensionName: String = _

  /**
    * 过滤源数据使用的where条件，仅当filterSource方法未在子类中重载时有效
    * 应使用维度表字段名
    */
  var sourceFilterWhere: String = _

  /**
    * 维度表字段与源数据字段的对应关系，仅当filterSource方法未在子类中重载时有效
    * key是维度表字段名，value是数据源中获取方式，支持spark sql表达
    */
  var sourceColumnMap: Map[String, String] = Map()

  /**
    * 是否执行全量更新
    * 在全量更新模式下，输入数据必须是当前状态下的维度全量数据，原来的维度信息存在的行如果再新源数据中不存在会被标记为失效
    * 增量模式下，输入数据可以只包含变化的行。增量模式下不能增加列
    */
  var fullUpdate: Boolean = false

  /**
    * 增量更新时间字段，只在增量更新模式下有用
    */
  var sourceTimeCol: String = "update_time"


  override def extract(params: Params): DataFrame = {
    //初始化参数处理和验证
    valid(params)

    //读取源数据
    val sourceDf =
      if (fullUpdate) {
        readSource(readSourceType)
      } else {
        readSourceIncr(readSourceType)
      }

    //过滤源数据
    val filteredSourceDf = filterSource(sourceDf)

    filteredSourceDf.persist()

    if (!fullUpdate) {
      val count = filteredSourceDf.count()
      if (count == 0) {
        println("源数据为空。。。。。。。。")
        return null
      } else {
        println("源数据更新条数：" + count)
      }
    }


    //过滤后源数据主键唯一性判断和处理
    checkPrimaryKeys(filteredSourceDf, columns.primaryKeys)

    println("成功获取源数据")
    if (debug) filteredSourceDf.show

    filteredSourceDf
  }

  /**
    * 验证参数，优化配置
    */
  private def valid(params: Params): Unit = {
    if (params.mode == "all") {
      fullUpdate = true
    } //不支持通过参数设置为增量更新模式，因为不是所有维度都支持增量更新

    columns.trackingColumns = if (columns.trackingColumns == null) List() else columns.trackingColumns
    if (columns.primaryKeys == null || columns.primaryKeys.isEmpty) {
      throw new RuntimeException("业务主键未设置！")
    }
    if (columns.getSourceColumns == null || columns.getSourceColumns.isEmpty) {
      throw new RuntimeException("维度表列未设置！")
    }
    (columns.primaryKeys ++ columns.trackingColumns).foreach(s =>
      if (!columns.getSourceColumns.contains(s)) {
        throw new RuntimeException("列" + s + "应该同时在allColumns中添加")
      }
    )
  }

  /**
    * 自定义的源数据读取方法
    * 默认是从jdbc中读取，如需自定义，可以在子类中重载实现
    *
    * @return
    */
  def readSource(readSourceType: Value): DataFrame = {
    if (readSourceType == null || readSourceType == jdbc) {
      sqlContext.read.format("jdbc").options(sourceDb).load()
    } else {
      null
    }
  }

  def readSourceIncr(readSourceType: Value): DataFrame = {
    if (readSourceType == null || readSourceType == jdbc) {
      val date = DateUtils.truncate(DateUtils.addDays(new Date(), -3), Calendar.HOUR_OF_DAY)
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val dateStr = sdf.format(date)
      sqlContext.read.format("jdbc").options(sourceDb).load().where(s"$sourceTimeCol >= '$dateStr'")
    } else {
      null
    }
  }

  /**
    * 处理原数据的自定义的方法
    * 默认可以通过配置实现，如果需要自定义处理逻辑，可以再在子类中重载实现
    *
    * @param sourceDf
    * @return
    */
  def filterSource(sourceDf: DataFrame): DataFrame = {
    val filtered = sourceDf.selectExpr(columns.getSourceColumns.map(
      s => if (sourceColumnMap.contains(s))
        sourceColumnMap(s) + " as " + s
      else s
    ): _*)
    if (sourceFilterWhere != null) filtered.where(sourceFilterWhere) else filtered
  }

  private def checkPrimaryKeys(sourceDf: DataFrame, primaryKeys: List[String]): DataFrame = {

    val primaryKeyNullCount = sourceDf.where(primaryKeys.map(s => s + " is null").mkString(" or ")).count
    if (primaryKeyNullCount > 0) {
      throw new RuntimeException("存在业务主键是null")
    }

    val duplicatePkDf = sourceDf.groupBy(
      primaryKeys.map(s => col(s)): _*
    ).agg(
      count("*").as("count")
    ).where("count > 1")

    //当前在出现重复主键时直接报错
    val duplicatePkCount = duplicatePkDf.count()
    if (duplicatePkCount > 0) {
      duplicatePkDf.dropDuplicates(primaryKeys).selectExpr(primaryKeys: _*).show
      throw new RuntimeException("存在重复业务主键" + duplicatePkCount + "个！部分展示如上")
    }
    sourceDf
  }

  override def transform(params: Params, filteredSourceDf: DataFrame): DataFrame = {

    if (filteredSourceDf == null) return null

    val onlineDimensionDir = DIMENSION_HDFS_BASE_PATH + File.separator + dimensionName

    //首次创建维度
    if (!HdfsUtil.fileIsExist(onlineDimensionDir, "_SUCCESS")) { //判断维度表路径下有没有success文件
      if (!fullUpdate) {
        throw new RuntimeException("维度第一次创建，请使用全量模式")
      }
      val result = DataFrameUtil.dfZipWithIndex(
        DataFrameUtil.addDimTime(
          filteredSourceDf.selectExpr(columns.getSourceColumns :_*),
          DimensionBase.defaultValidTime,
          null),
        columns.skName
      )
      return result
    }

    val today = new Date()

    //读取现有维度
    val originalDf = sqlContext.read.parquet(onlineDimensionDir)

    println("成功获取现有维度")
    if (debug) originalDf.show

    val newColumns = {
      val index = new ListBuffer[Int]
      originalDf.schema.fields.map(_.name).foreach(s => index += columns.getSourceColumns.indexOf(s))
      columns.getSourceColumns.filter(s => !index.contains(columns.getSourceColumns.indexOf(s)))
    }

    if (debug) println("新增加列：" + newColumns)

    if (!fullUpdate && newColumns.nonEmpty) {
      throw new RuntimeException("有新增列，必须用全量更新模式运行")
    }

    //新增的行，业务键在新的源数据中存在但是在之前的维度中不存在的行
    val addDf =
      filteredSourceDf.as("b").join(
        originalDf.where(columns.invalidTimeKey + " is null").as("a"), columns.primaryKeys, "leftouter"
      ).where(
        "a." + columns.skName + " is null"
      ).selectExpr(
        columns.getSourceColumns.map(s => "b." + s): _*
      )

    println("成功获取到新增的行")
    if(debug) {
      addDf.show
      println("新增行数为："+addDf.count)
    }

    //找到追踪列变化的信息包含的dataframe。用于增加追踪列变化的行以及将变化前的行标注为失效
    val changedTrackingColumnDf =
      filteredSourceDf.as("b").join(
        originalDf.where(columns.invalidTimeKey + " is null").as("a"), columns.primaryKeys, "leftouter"
      ).where(
        if (columns.trackingColumns == null || !columns.trackingColumns.exists(!newColumns.contains(_))) {
          "1!=1"
        } else {
          //若trackingColumn原本为null，不增加新行 TODO 当前逻辑又非null变成null也不增加新行
          columns.trackingColumns.filter(!newColumns.contains(_)).map(s => s"a.$s != b.$s").mkString(" or ")
        }
      )
    println("成功找到追踪列变化的信息")
    if(debug) changedTrackingColumnDf.show

    //更新后维度表中需要添加的行，包括新增的和追踪列变化的
    val extendDf =
      if (columns.trackingColumns == null || columns.trackingColumns.isEmpty) {
        DataFrameUtil.addDimTime(addDf, DimensionBase.defaultValidTime, null)
      } else {
        DataFrameUtil.addDimTime(addDf, DimensionBase.defaultValidTime, null).unionAll(
          //因为追踪列变化而新增的行
          DataFrameUtil.addDimTime(
            changedTrackingColumnDf.selectExpr(columns.getSourceColumns.map(s => s"b.$s"): _*),
            today, null)
        )
      }

    println("计算完成需要增加的行")
    if (debug) extendDf.show

    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val todayStr = sdf.format(today)


    //现有维度表中已经存在的行，已经根据现有源信息做了字段更新
    //因为追踪历史记录的列变化导致的失效的没有处理，在下一部分处理
    //若trackingColumn原本为null，源数据有值后，会在直接在该主键的所有行上变更，也就是说，历史记录默认不记录null值
    //如果维度表中已经存在的业务键在新的源信息中被删除了，则会保留维度表中的值。在全量更新的模式下，会添加上失效时间，在增量模式下不会更新失效时间
    val originalExistDf = originalDf.as("a").join(
      filteredSourceDf.as("b"),
      columns.primaryKeys.map(s => originalDf(s) === filteredSourceDf(s)).reduceLeft(_ && _),
      "leftouter"
    ).selectExpr(List("a." + columns.skName)
      ++ columns.getSourceColumns.map(s => {
      if (newColumns.contains(s)) s"b.$s"
      else if (columns.primaryKeys.contains(s)) s"a.$s"
      else if (columns.trackingColumns.contains(s)) s"CASE WHEN a.$s is not null THEN a.$s ELSE b.$s END as $s"
      else "CASE WHEN b." + columns.primaryKeys.head + s" is not null THEN b.$s ELSE a.$s END as $s"
    })
      ++ List(columns.validTimeKey).map(s => "a." + s)
      ++ List(columns.invalidTimeKey).map(s =>
      if (fullUpdate) {
        //更新filteredSourceDf中不存在的行，使其失效
        "cast(CASE WHEN b." + columns.primaryKeys.head + s" is null and a.$s is null " +
          s"THEN '$todayStr' ELSE a.$s END as timestamp) as $s"
      } else {
        //增量模式，可以允许filteredSourceDf只包含源数据中变动的行
        //同时要注意，在需要增加列时，filteredSourceDf必须要包含完整源数据
        "a." + s
      })
      : _*
    )
    println("originalExistDf:")
    if(debug) originalExistDf.show

    //现有维度表中已经存在的行，已经根据现有源信息做了字段更新，并且更新了dim_invalid_time
    val df =
      if (columns.trackingColumns == null || columns.trackingColumns.isEmpty) {
        originalExistDf
      } else {
        //变更后需要标注失效时间的行，包含代理键和失效时间两列
        val invalidColumnsDf =
          changedTrackingColumnDf.selectExpr(List("a." + columns.skName)
            ++ List("cast('" + todayStr + "' as timestamp) as " + columns.invalidTimeKey): _*)

        println("计算完成需要变更失效时间的行")
        if (debug) invalidColumnsDf.show

        //更新失效时间
        originalExistDf.as("origin").join(invalidColumnsDf.as("invalid"), List(columns.skName), "leftouter"
        ).selectExpr(
          List(columns.skName) ++ columns.getSourceColumns ++ List(columns.validTimeKey)
            ++ List("cast(CASE WHEN invalid." + columns.invalidTimeKey + " is not null THEN invalid." + columns.invalidTimeKey
            + " ELSE origin." + columns.invalidTimeKey + " END as timestamp) as " + columns.invalidTimeKey): _*
        )
      }

    println("计算完成原有维度数据更新后")
    if (debug) df.show

    //合并上述形成最终结果
    val offset =
      if (df.count() > 0) {
        df.agg(max(columns.skName)).first().getLong(0)
      } else {
        println("WARN! 原维度表为空")
        0
      }

    var unionAllResult = df.unionAll(
      DataFrameUtil.dfZipWithIndex(
        extendDf
        , columns.skName
        , offset
      )
    )

    println("计算完成最终生成的新维度")
    if (debug) unionAllResult.show

    if (columns.addColumns != null) {
      columns.addColumns.foreach(column =>
        unionAllResult = unionAllResult.withColumn(column.name, column.udf(column.inputColumns.map(col): _*))
      )
    }
    //关联其他维度，支持雪花模型
    val linkDimension = parseDimension(unionAllResult, columns.linkDimensionColumns, columns.skName, columns.invalidTimeKey,DIMENSION_HDFS_BASE_PATH)

    var result =
      if (linkDimension == null) {
        unionAllResult
      } else {
        val r = unionAllResult.join(linkDimension, List(columns.skName), "leftouter")
        if (debug) r.show
        r
      }

    if (columns.addColumns != null) {
      columns.addColumns.foreach(column =>
        if (!column.remainInFinal)
          result = result.drop(column.name)
      )
    }
    result
  }

  override def load(params: Params, df: DataFrame): Unit = {
    backup(params, df, dimensionName)
  }

  /**
    * 用来备份维度数据，然后将维度数据生成在临时目录，当isOnline参数为true的时候，将临时目录的数据替换线上维度
    *
    * @param p  the main args
    * @param df the DataFrame from execute function
    * @return a Unit.
    */
  private def backup(p: Params, df: DataFrame, dimensionType: String): Unit = {

    if (df == null) {
      return
    }
    val cal = Calendar.getInstance
    val date = DateFormatUtils.readFormat.format(cal.getTime)
    val onLineDimensionDir = DIMENSION_HDFS_BASE_PATH + File.separator + dimensionType
    val onLineDimensionBackupDir = DIMENSION_HDFS_BASE_PATH_BACKUP + File.separator + date + File.separator + dimensionType
    val onLineDimensionDirTmp = DIMENSION_HDFS_BASE_PATH_TMP + File.separator + dimensionType
    val onLineDimensionDirDelete = DIMENSION_HDFS_BASE_PATH_DELETE + File.separator + dimensionType
    println("线上数据目录:" + onLineDimensionDir)
    println("线上数据备份目录:" + onLineDimensionBackupDir)
    println("线上数据临时目录:" + onLineDimensionDirTmp)
    println("线上数据等待删除目录:" + onLineDimensionDirDelete)

    df.persist(StorageLevel.MEMORY_AND_DISK)
    val isOnlineFileExist = HdfsUtil.IsDirExist(onLineDimensionDir)
    if (isOnlineFileExist) {
      val isBackupExist = HdfsUtil.IsDirExist(onLineDimensionBackupDir)
      if (isBackupExist) {
        println("数据已经备份,跳过备份过程")
      } else {
        println("生成线上维度备份数据:" + onLineDimensionBackupDir)
        val isSuccessBackup = HdfsUtil.copyFilesInDir(onLineDimensionDir, onLineDimensionBackupDir)
        println("备份数据状态:" + isSuccessBackup)
      }
    } else {
      println("无可用备份数据")
    }

    //防止文件碎片
    val total_count = BigDecimal(df.count())
    val partition = Math.max(1, (total_count / THRESHOLD_VALUE).intValue())
    println("repartition:" + partition)

    val isTmpExist = HdfsUtil.IsDirExist(onLineDimensionDirTmp)
    if (isTmpExist) {
      println("删除线上维度临时数据:" + onLineDimensionDirTmp)
      HdfsUtil.deleteHDFSFileOrPath(onLineDimensionDirTmp)
    }
    println("生成线上维度数据到临时目录:" + onLineDimensionDirTmp)
    df.orderBy(columns.skName).repartition(partition).write.parquet(onLineDimensionDirTmp)

    println("数据是否上线:" + p.isOnline)
    if (p.isOnline) {
      println("数据上线:" + onLineDimensionDir)
      if (isOnlineFileExist) {
        println("移动线上维度数据:from " + onLineDimensionDir + " to " + onLineDimensionDirDelete)
        val isRenameSuccess = HdfsUtil.rename(onLineDimensionDir, onLineDimensionDirDelete)
        println("isRenameSuccess:" + isRenameSuccess)
      }

      val isOnlineFileExistAfterRename = HdfsUtil.IsDirExist(onLineDimensionDir)
      if (isOnlineFileExistAfterRename) {
        throw new RuntimeException("rename failed")
      } else {
        val isSuccess = HdfsUtil.rename(onLineDimensionDirTmp, onLineDimensionDir)
        println("数据上线状态:" + isSuccess)
      }
      println("删除过期数据:" + onLineDimensionDirDelete)
      HdfsUtil.deleteHDFSFileOrPath(onLineDimensionDirDelete)
    }

  }
}

object DimensionBase {
  val defaultValidTime: Date = DateUtils.parseDate("2000-01-01", "yyyy-MM-dd")
}
