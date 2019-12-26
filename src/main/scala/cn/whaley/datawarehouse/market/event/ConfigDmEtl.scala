package cn.whaley.datawarehouse.market.event

import java.io.File

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition, UserDefinedColumn}
import cn.whaley.datawarehouse.exception.NoSourceDataException
import cn.whaley.datawarehouse.global.Globals._
import cn.whaley.datawarehouse.market.event.constant.Constants._
import cn.whaley.datawarehouse.util.{HdfsUtil, Params, UdfUtils}
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.expr
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.io.Source


/**
  * @author wangning
  * @date 2019/9/6 15:38
  */
object ConfigDmEtl extends MarketEtlBase {

  private val interfaceUrl = "http://bigdata-app.whaley.cn/matrix/api/remote/dim/get?id="
  private var interfaceData: JSONObject = null

  def initParam(params: Params): Unit = {

    UdfUtils.register(spark)

    val factId = params.factId
    val factCol = params.factColumn
    interfaceData = JSON.parseObject(getDataFromInterface(factId)).getJSONObject("data") //读取接口数据


    val tableData = interfaceData.getJSONObject("table") //获取table配置值
    topicName = tableData.getString("tableName")

    factTime = tableData.getString("dimTime")

    uniqueLogIdColumn = tableData.getString("uniqueLogIdColumn")

    source = tableData.getString("source")
    //source默认值为default
    if (source == null || source.isEmpty) {
      source = "default"
    }

    //增加新的字段
    interfaceData.getJSONArray("addColumns") //获取addColumns配置值
      .toArray.foreach(data => {
      val columnName = JSON.parseObject(data.toString).getString("columnName")
      val parseSql = JSON.parseObject(data.toString).getString("parseSql")
      if (factCol.contains("default") && factCol.length == 1) {
        addColumns = addColumns ::: UserDefinedColumn(columnName, null, null, parseSql) :: Nil
      } else {
        factCol.foreach(c => {
          if (c == columnName) {
            addColumns = addColumns ::: UserDefinedColumn(columnName, null, null, parseSql) :: Nil
          } else
            null
        })
      }
    })

    if (debug) println("addColumns:" + addColumns)

    //关联维度表获取sk值
    val dimensionTableNames: ListBuffer[String] = new ListBuffer[String]
    interfaceData.getJSONArray("dimensionColumns") //获取dimensionColumns配置值
      .toArray.foreach(f = data => {
      var DimensionJoinConditionList: List[DimensionJoinCondition] = Nil
      val columnName = JSON.parseObject(data.toString).getString("columnName")
      val dimensionTableName = JSON.parseObject(data.toString).getString("dimensionTableName")
      var dimensionTableUniqueName = JSON.parseObject(data.toString).getString("dimensionTableUniqueName")
      if (dimensionTableUniqueName == null || dimensionTableUniqueName.trim.isEmpty) {
        dimensionTableUniqueName = dimensionTableName
      }
      dimensionTableNames.+=(dimensionTableUniqueName)
      val dimensionSkColumnName = JSON.parseObject(data.toString).getString("dimensionSkColumnName")
      //获取关联条件
      JSON.parseObject(data.toString).getJSONArray("dimJoinConditions")
        .toArray.foreach(data => {
        val columnPairs = JSON.parseObject(data.toString).getJSONObject("columnPairs")
        val columnPairsMap: Map[String, String] =
          columnPairs.map(e => (e._1, e._2.toString)).toMap
        val dimWhereClause = JSON.parseObject(data.toString).getString("dimWhereClause")
        val sourceWhereClause = JSON.parseObject(data.toString).getString("sourceWhereClause")
        DimensionJoinConditionList = DimensionJoinConditionList :::
          DimensionJoinCondition(columnPairsMap, dimWhereClause, null, sourceWhereClause) :: Nil //将多个关联条件放到list里
      })
      dimensionColumns = dimensionColumns :::
        new DimensionColumn(dimensionTableName, dimensionTableUniqueName, DimensionJoinConditionList, dimensionSkColumnName, columnName) :: Nil
    })

    if (debug) println(s"dimensionColumns:$dimensionColumns")

    //保留哪些列，以及别名声明
    partition = 0
    interfaceData.getJSONArray("dimColumns") //获取factColumns配置值
      .toArray.foreach(data => {
      val columnName = JSON.parseObject(data.toString).getString("columnName")
      val expression = JSON.parseObject(data.toString).getString("expression")
      if (factCol.contains("default") && factCol.length == 1) {
        columnsFromSource = columnsFromSource ::: (columnName, expression) :: Nil
      } else {
        factCol.foreach(f => {
          if (expression.contains(f)) {
            columnsFromSource = (columnsFromSource ::: (columnName, expression) :: Nil).distinct
          } else
            null
        })
      }
    })
    if (debug) println(s"columnsFromSource:$columnsFromSource")

    //解析事实字段中使用到的维度表
    val factExpressions = interfaceData.getJSONArray("dimColumns").map(data => JSON.parseObject(data.toString).getString("expression"))
    val pattern = "dim_\\w+\\.".r
    dimensionsNeedInFact = factExpressions.flatMap(exp => {
      pattern.findAllIn(exp)
    }).distinct.toList.map(s => s.replace(".", ""))

    println(s"dimensionsNeedInFact:$dimensionsNeedInFact")

    //关联多次的并且没有重命名的维度表
    val duplicateDim = dimensionTableNames.flatMap(_.split(" ")).map((_, 1)).groupBy(_._1).map(t => (t._1, t._2.size)).filter(t => t._2 > 1).keys

    dimensionsNeedInFact.foreach(dim => {
      if (duplicateDim.contains(dim)) {
        throw new RuntimeException(s"在事实表中使用的维度表${dim}需要定义别名")
      }
    })
    //检查大宽表中字段是否与维度表中字段重名
    if (dimensionColumns.nonEmpty) {
      var equalColumns: ListBuffer[String]= ListBuffer()
      dimensionColumns.foreach(dc => {
        val dimDf = sqlContext.read.parquet(DM_DIMENSION_HDFS_BASE_PATH + File.separator + dc.dimensionName)
         equalColumns = isDuplicateName(columnsFromSource,dimDf.columns.toList,equalColumns)
      })
      if(equalColumns.nonEmpty){
        throw new RuntimeException(s"${equalColumns.mkString("{",",","}")} 字段与维度表中的字段重名，请起别名。。。")
      }
    }
  }

  /**
    * 源数据读取函数, ETL中的Extract
    * 如需自定义，可以在子类中重载实现
    *
    * @return
    */
  override def extract(params: Params): DataFrame = {

    params.paramMap.get("date") match {
      case Some(d) => {
        println("数据时间：" + d)
        if (partition == 0) {
          readSource(d.toString, params.startHour, params)
        } else {
          readSource(d.toString, params.startHour, params).repartition(partition)
        }
      }
      case None =>
        throw new RuntimeException("未设置时间参数！")
    }
  }

  /**
    * step 1, 获取源数据
    **/
  def readSource(sourceDate: String, sourceHour: String, params: Params): DataFrame = {
    val factCol = params.factColumn
    val result = if (factCol.contains("default") && factCol.length == 1) {
      readSourceFromOds(sourceDate, sourceHour)
    } else {
      readSourceFromParquet(params)
    }
    println("成功读取到源数据。。。")
    if (debug) result.show
    result
  }

  /**
    * 数据转换函数，ETL中的Transform
    *
    * @return
    */
  override def transform(params: Params, sourceDf: DataFrame): DataFrame = {
    val factCol = params.factColumn
    if (factCol.contains("default") && factCol.length == 1) {
      super.transform(params, sourceDf)
    } else {
      updateFactColumns(params, sourceDf)
    }
  }


  /**
    * ETL过程执行程序
    */
  override def execute(params: Params): Unit = {
    initParam(params)
    super.execute(params)
  }


  def getDataFromInterface(id: String): String = {
    val data = Source.fromURL(interfaceUrl + id).mkString
    data
  }

  /**
    * 读取来自中间结果的源数据
    * @param params
    * @return
    */
  private def readSourceFromParquet(params: Params): DataFrame = {

    val completePath = getPath(params, DM_FACT_HDFS_BASE_PATH_COMPLETE)
    val completePathExist = HdfsUtil.IsDirExist(completePath)
    if (completePathExist) {
      sqlContext.read.parquet(completePath)
    } else {
      throw new RuntimeException(s"$completePath 不存在")
    }
  }

  /**
    * 读取来自dw_facts/dw_dimensions库下的中的源数据
    *
    * @param sourceDate
    * @param sourceHour
    * @return
    */
  private def readSourceFromOds(sourceDate: String, sourceHour: String): DataFrame = {
    val sourcesArray = interfaceData.getJSONArray("sources").toArray //获取sources配置值
    var extractSql = JSON.parseObject(sourcesArray(0).toString).getString("extractSql")
    extractSql = getFixSql(extractSql, sourceDate, sourceHour)
    if (debug) {
      println("读取源数据sql如下：")
      println(extractSql)
    }

    var mergerDf: DataFrame = sqlContext.sql(extractSql)

    for (i <- 1 until sourcesArray.length) {
      extractSql = JSON.parseObject(sourcesArray(i).toString).getString("extractSql")
      extractSql = getFixSql(extractSql, sourceDate, sourceHour)
      println(extractSql)
      mergerDf = mergerDf.union(sqlContext.sql(extractSql))
    }
    //当读入的数据源为空时，退出应用程序
    if (mergerDf.count() == 0) {
      throw new NoSourceDataException(s"The readSource method can not read any data by use parameters(sourceDate = $sourceDate ,sourceHour = $sourceHour, extractSql = $extractSql")
    }

    mergerDf
  }

  /**
    * 更新事实表中维度方法
    *
    * @param params
    * @param sourceDf
    * @return
    */
  private def updateFactColumns(params: Params, sourceDf: DataFrame): DataFrame = {

    var dimensionJoinDf: DataFrame = null
    var factTableDf: DataFrame = null
    var updateColumnDf: DataFrame = null
    val factCol = params.factColumn
    var sourceColumnLists: List[String] = Nil
    var dimensionColumnList: List[DimensionColumn] = Nil

    if (uniqueLogIdColumn == null || uniqueLogIdColumn.trim.isEmpty) {
      uniqueLogIdColumn = INDEX_NAME
    }

    if (debug) println(s"唯一索引列:$uniqueLogIdColumn")
    //判断需要更新或者增加的维度
    val sourcecompleteDf =
      if (!factCol.forall(_.endsWith("_sk")) && !factCol.exists(_ == "default")) {
        val addColumnDf = updateOrAddColumns(params, sourceDf)
        if (columnsFromSource.nonEmpty)
          updateColumnDf = addColumnDf.selectExpr(factCol.filter(!_.endsWith("_sk")).toList ++ List(uniqueLogIdColumn): _*)
        addColumnDf
      } else {
        sourceDf
      }
    sourcecompleteDf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //事实表行数
    dfLineCount = sourcecompleteDf.count
    println(s"完整事实表行数为：$dfLineCount")

    if (debug) println(s"sourcecompleteDf:${sourcecompleteDf.printSchema()}")
    if (debug && updateColumnDf != null) println(s"updateColumnDf:${updateColumnDf.printSchema()}")

    //读取事实表
    val factTablePath = getPath(params, DM_FACT_HDFS_BASE_PATH)
    val factTablePathExist = HdfsUtil.IsDirExist(factTablePath)
    if (factTablePathExist) {
      factTableDf = sqlContext.read.parquet(factTablePath)
    } else {
      throw new RuntimeException("读取线上事实表路径有误。。。")
    }

    factTableDf.persist(StorageLevel.MEMORY_AND_DISK_SER)

    if (debug) {
      println("线上事实表完整schema信息如下：")
      factTableDf.printSchema()
    }
    //删除需要更新的代理键sk
    factCol.foreach(fc => {
      factTableDf = factTableDf.drop(fc)
    })
    //删除需要更新的事实表字段
    if (columnsFromSource.nonEmpty) {
      columnsFromSource.foreach(cf => {
        factTableDf = factTableDf.drop(cf._1)
      })
    }

    if (debug) println(s"删除以后的事实表schema:${factTableDf.printSchema()}")

    if (updateColumnDf != null) {
      factTableDf = factTableDf.join(updateColumnDf, List(uniqueLogIdColumn), "leftouter")
      if (debug) println(s"关联更新字段以后的事实表schema:${factTableDf.printSchema()}")
    }
    //对需要更新的代理键进行维度解析
    if (factCol.exists(_.endsWith("_sk")) && dimensionColumns.nonEmpty) {
      dimensionColumns.foreach(c => {
        factCol.toList.foreach(fc => {
          if (fc == c.factSkColumnName) {
            dimensionColumnList = dimensionColumnList ::: new DimensionColumn(c.dimensionName, c.dimensionNameAs, c.joinColumnList, c.dimensionSkName, c.factSkColumnName) :: Nil
          }
        })
      })
      if (debug) println(s"dimensionColumnList:$dimensionColumnList")

      if (dimensionColumnList != null && dimensionColumnList.nonEmpty) {
        dimensionColumnList.foreach(dc => {
          dc.joinColumnList.foreach(jc => {
            sourceColumnLists = sourceColumnLists ::: jc.columnPairs.keys.toList
          })
        })
      }
      val sourceFilterDf = sourcecompleteDf.selectExpr(
        (sourceColumnLists ++ List(uniqueLogIdColumn)).distinct: _*)
      if (debug) println(s"sourceFilterDf:${sourceFilterDf.printSchema()}")

      dimensionJoinDf = parseDimension(sourceFilterDf, dimensionColumnList, uniqueLogIdColumn, factTime,DM_DIMENSION_HDFS_BASE_PATH)
      factTableDf = factTableDf.join(dimensionJoinDf, List(uniqueLogIdColumn), "leftouter")
      if (debug) println(s"更新的sk代理键join到事实表中schema:${factTableDf.printSchema()}")
    }

    sourcecompleteDf.unpersist()

    // 需要关联用到的维度
    factTableDf = factTableDf.as("source")
    var result = factTableDf
    if (columnsFromSource.nonEmpty && dimensionColumns != null && dimensionsNeedInFact != null) {
      dimensionColumns.foreach(c => {
        if (columnsFromSource.exists(_._2.contains(c.dimensionNameAs)) && dimensionsNeedInFact.contains(c.dimensionNameAs)) {
          val dimensionDf = sqlContext.read.parquet(DM_DIMENSION_HDFS_BASE_PATH + File.separator + c.dimensionName)
          result = result.join(dimensionDf.as(c.dimensionNameAs),
            expr("source." + c.factSkColumnName + " = " + c.dimensionNameAs + "." + c.dimensionSkName), "leftouter")
        }
      })
      if (debug) println(s"需要关联用到的维度表schema:${result.printSchema()}")

      columnsFromSource.foreach(c => {
        if (c._2.contains("(") || c._2.contains(" ") || c._2.contains(".")) {
          result = result.selectExpr(columnsFromSource.map(c => c._2 + " as " + c._1) ++ factTableDf.columns.map("source." + _): _*)
          updateColumnDf.columns.filter(_ != uniqueLogIdColumn).foreach(c => {
            result = result.drop(c)
          })
        } else {
          result = result.withColumnRenamed(c._2, c._1)
        }
      })
    }

    if (debug) {
      println(s"最终结果schema:${result.printSchema}")
      result.show(false)
    }
    result
  }

  /**
    * 获取事实表中需要更新的字段
    *
    * @param params
    * @param sourceDf
    * @return
    */
  private def updateOrAddColumns(params: Params, sourceDf: DataFrame): DataFrame = {
    var result = sourceDf
    val factCol = params.factColumn
    factCol.foreach(c => {
      result = result.drop(c)
    })
    if (debug) println(s"删除后的schema:${result.printSchema()}")
    if (addColumns != null) {
      addColumns.foreach(udc => {
        if (udc.expression != null) {
          result = result.withColumn(udc.name, expr(udc.expression))
        }
      })
    }
    if (debug) println(s"更新以后的schema:${result.printSchema()}")
    val completePath = getPath(params, DM_FACT_HDFS_BASE_PATH_COMPLETE_TMP)
    val completePathExist = HdfsUtil.IsDirExist(completePath)
    if (completePathExist) {
      HdfsUtil.deleteHDFSFileOrPath(completePath)
    }
    //防止文件碎片
    val load_to_hdfs_partition = repartition(result)
    result.repartition(load_to_hdfs_partition).write.parquet(completePath)
    println("中间结果数据成功更新到tmp目录下。。。。")
    result
  }

  /**
    * 修复sql，所有的表的参数进行替换
    * @param sql
    * @param date
    * @param hour
    * @return
    */
  def getFixSql(sql: String, date: String, hour: String): String = {
    var fixSql = sql
    fixSql = fixSql.replace("$day_p", date).replace("${day_p}", date)
    if (hour != null) {
      fixSql = fixSql.replace("$hour_p", hour).replace("${hour_p}", hour)
    }
    fixSql
  }

  /**
    * 检查大宽表中的字段名是否与维度表中重名
    * @param columnsFromSource
    * @param dimColumns
    * @param equalColumns
    * @return
    */
  def isDuplicateName(columnsFromSource: List[(String, String)], dimColumns: List[String],equalColumns:ListBuffer[String]): ListBuffer[String] = {
    if (columnsFromSource.nonEmpty && dimColumns.nonEmpty) {
      columnsFromSource.foreach(c => {
        if (dimColumns.contains(c._1)) {
          equalColumns.append(c._1)
        }
      })
    }
    equalColumns
  }
}
