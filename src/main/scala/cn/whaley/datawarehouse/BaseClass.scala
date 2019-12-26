package cn.whaley.datawarehouse

import cn.whaley.datawarehouse.common.DimensionColumn
import cn.whaley.datawarehouse.exception.NoSourceDataException
import cn.whaley.datawarehouse.global.Constants._
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.{Params, ParamsParseUtil}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.io.File


/**
  * Created by Tony on 16/12/21.
  */
trait BaseClass {
  val config = new SparkConf()
  /**
    * define some parameters
    */
  var spark: SparkSession = null
  var sc: SparkContext = null
  implicit var sqlContext: SQLContext = null

  var readSourceType: Value = _

  var debug = false

  /**
    * 程序入口
    *
    * @param args
    */
  def main(args: Array[String]) {
    try {
      println("init start ....")
      init()
      println("init success ....")

      beforeExecute()
      println("execute start ....")
      ParamsParseUtil.parse(args) match {
        case Some(p) => {
          println(p)
          if (p.startDate != null) {
            val date = p.startDate
            p.paramMap.put("date", date)
          }
          if (p.mode != null) {
            if (!List("all", "increment").contains(p.mode)) {
              throw new RuntimeException("mode must be all or increment")
            }
          }
          execute(p)
        }
        case None => {
          throw new RuntimeException("parameters wrong")
        }
      }
      println("execute end ....")
    } catch {
      case noSourceDataException: NoSourceDataException => noSourceDataException.printStackTrace()
      case e: Exception => throw e
    } finally {
      destroy()
    }


  }

  /**
    * 全局变量初始化
    */
  def init(): Unit = {
    spark = SparkSession.builder()
      .config(config)
      .enableHiveSupport()
      .getOrCreate()
    sc = spark.sparkContext
    sqlContext = spark.sqlContext
  }

  def beforeExecute(): Unit = {

  }

  /**
    * ETL过程执行程序
    */
  def execute(params: Params): Unit = {

    if (params.debug) debug = true

    val df = extract(params)

    val result = transform(params, df)

    load(params, result)

  }

  /**
    * release resource
    */
  def destroy(): Unit = {
    if (sc != null) {
      try {
        sqlContext.clearCache()
      } catch {
        case e: Throwable =>
      }
      sc.stop()
    }
  }

  /**
    * 源数据读取函数, ETL中的Extract
    * 如需自定义，可以在子类中重载实现
    *
    * @return
    */
  def extract(params: Params): DataFrame

  /**
    * 数据转换函数，ETL中的Transform
    *
    * @return
    */
  def transform(params: Params, df: DataFrame): DataFrame

  /**
    * 数据存储函数，ETL中的Load
    */
  def load(params: Params, df: DataFrame)

  /**
    * 维度解析方法
    *
    * @param sourceDf         目标表
    * @param dimensionColumns 解析用的join参数
    * @param uniqueKeyName    目标表的唯一键列
    * @param sourceTimeColumn 源数据时间列获取sql(或者只是个列名)
    * @return 输出包含uniqueKeyName列和所以维度表的代理键列，不包含目标表中的数据，失败返回null
    */
  def parseDimension(sourceDf: DataFrame,
                     dimensionColumns: List[DimensionColumn],
                     uniqueKeyName: String,
                     sourceTimeColumn: String = null,
                     dimensionFromHdfsPath: String): DataFrame = {
    var dimensionColumnDf: DataFrame = null
    if (dimensionColumns != null) {
      //对每个维度表
      dimensionColumns.foreach(c => {
        val dimensionDfBase = sqlContext.read.parquet(dimensionFromHdfsPath + File.separator + c.dimensionName)
        var df: DataFrame = null
        //对每组关联条件
        c.joinColumnList.foreach(jc => {
          var dimensionDf = dimensionDfBase
          //维度表过滤
          if (jc.whereClause != null && !jc.whereClause.isEmpty) {
            dimensionDf = dimensionDf.where(jc.whereClause)
          }
          //维度表排序
          if (jc.orderBy != null && jc.orderBy.nonEmpty) {
            dimensionDf = dimensionDf.orderBy(jc.orderBy.map(s => if (s._2) col(s._1).desc else col(s._1).asc): _*)
          }

          //维度表字段筛选
          dimensionDf = dimensionDf.selectExpr(
            (jc.columnPairs.values.toList ++ c.dimensionColumnName.map(_._1)
              ++ List("dim_valid_time", "dim_invalid_time")
              ).distinct: _*)

          //维度表去重
          //          dimensionDf = dimensionDf.dropDuplicates(jc.columnPairs.values.toArray)
          //实时表源数据过滤
          var sourceFilterDf =
          if (jc.sourceWhereClause != null && !jc.sourceWhereClause.isEmpty)
            sourceDf.where(jc.sourceWhereClause)
          else
            sourceDf

          //增加时间字段
          sourceFilterDf =
            if (sourceTimeColumn == null || sourceTimeColumn.isEmpty) {
              sourceFilterDf.withColumn(COLUMN_NAME_FOR_SOURCE_TIME, expr("null"))
            } else {
              sourceFilterDf.withColumn(COLUMN_NAME_FOR_SOURCE_TIME, expr(sourceTimeColumn))
            }

          //源表字段筛选
          sourceFilterDf = sourceFilterDf.selectExpr(
            (jc.columnPairs.keys.toList ++ List(COLUMN_NAME_FOR_SOURCE_TIME, uniqueKeyName)).distinct: _*
          )

          //源数据中未关联上的行
          val notJoinDf =
            if (df != null) {
              sourceFilterDf.as("a").join(
                df.as("dim"), sourceFilterDf(uniqueKeyName) === df(uniqueKeyName), "leftouter"
              ).where(c.dimensionColumnName.map("dim." + _._1 + " is null").mkString(" and ")).selectExpr("a.*")
            } else {
              sourceFilterDf
            }

          //源表与维度表join
          val pairCondition =
            if (jc.columnPairs.nonEmpty)
              jc.columnPairs.map(s => notJoinDf(s._1) === dimensionDf(s._2)).reduceLeft(_ && _)
            else
              expr("1=1")

          val timeCondition =
            expr(s"a.$COLUMN_NAME_FOR_SOURCE_TIME is null and b.dim_invalid_time is null") ||
              expr(s"a.$COLUMN_NAME_FOR_SOURCE_TIME >= b.dim_valid_time and " +
                s"(a.$COLUMN_NAME_FOR_SOURCE_TIME < b.dim_invalid_time or b.dim_invalid_time is null)")

          val afterJoinDf = notJoinDf.as("a").join(
            dimensionDf.as("b"),
            pairCondition && timeCondition,
            "inner"
          ).selectExpr("a." + uniqueKeyName :: c.dimensionColumnName.map("b." + _._1): _*
          ).dropDuplicates(List(uniqueKeyName))

          /*
                    //源表与维度表join
                    val afterJoinDf = notJoinDf.as("a").join(
                      dimensionDf.as("b"),
                      (if (jc.columnPairs.nonEmpty)
                        jc.columnPairs.map(s => notJoinDf(s._1) === dimensionDf(s._2)).reduceLeft(_ && _)
                      else
                        expr("1=1"))
                          && (expr(s"a.$COLUMN_NAME_FOR_SOURCE_TIME is null and b.dim_invalid_time is null") ||
                          expr(s"a.$COLUMN_NAME_FOR_SOURCE_TIME >= b.dim_valid_time and " +
                            s"(a.$COLUMN_NAME_FOR_SOURCE_TIME < b.dim_invalid_time or b.dim_invalid_time is null)")),
                      "inner"
                    ).selectExpr("a." + uniqueKeyName :: c.dimensionColumnName.map("b." + _._1): _*
                    ).dropDuplicates(List(uniqueKeyName))
                    */

          if (df != null) {
            df = afterJoinDf.unionAll(df)
          } else {
            df = afterJoinDf
          }
        })
        df = sourceDf.select(uniqueKeyName).as("a").join(
          df.as("b"), sourceDf(uniqueKeyName) === df(uniqueKeyName), "leftouter"
        ).selectExpr(
          "a." + uniqueKeyName :: c.dimensionColumnName.map(c => "b." + c._1 + " as " + c._2): _*
        )
        //多个维度合成一个DataFrame
        if (dimensionColumnDf == null) {
          dimensionColumnDf = df
        } else {
          dimensionColumnDf = dimensionColumnDf.join(df, List(uniqueKeyName), "leftouter")
        }
      }
      )
    }
    dimensionColumnDf
  }
}
