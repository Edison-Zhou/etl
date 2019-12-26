package cn.whaley.datawarehouse.fact

import cn.whaley.datawarehouse.common.{DimensionColumn, DimensionJoinCondition, UserDefinedColumn}
import cn.whaley.datawarehouse.util.Params
import org.apache.spark.sql.{DataFrame}

import scala.io.Source
import scala.util.parsing.json.JSON

/**
  * Created by lituo on 2018/8/29.
  */
object ConfigFactEtlYang extends FactEtlBase {

  var dataObj: Map[String, String] = null

  def initParam(params: Params): Unit = {

     var id = params.factId
    /** 读取配置 **/
     val url: String = "http://test-bigdata-app.whaley.cn/matrix/api/remote/fact/get?id="+id
     val tableContent: String = Source.fromURL(url, "utf-8").mkString
    /** 获取一级json **/
     val jsonObj = JSON.parseFull(tableContent).get.asInstanceOf[Map[String, String]]
    /** 获取二级json，date的value json **/
     dataObj = jsonObj.get("data").get.asInstanceOf[Map[String, String]]

    /**
      * 获取事实表表名
      */
     topicName = dataObj.get("table").get.asInstanceOf[Map[String, String]].get("tableName").mkString

     factTime = null

    /**
      *  addColumns
      */
     var add_column = dataObj.get("addColumns").get.asInstanceOf[List[Map[String, String]]]
     var i = 0
     for (i <- 0 to add_column.length - 1) {
        addColumns =  UserDefinedColumn(add_column(i).get("columnName").mkString,null, null,add_column(i).get("parseSql").mkString) :: addColumns
     }

    /**
      * dimensionColumns
      */
     var dimension_column = dataObj.get("dimensionColumns").get.asInstanceOf[List[Map[String, String]]]
    /**循环得到多个sk**/
     for (i <- 0 to dimension_column.length - 1) {
         val columnName = dimension_column(i).get("columnName").mkString
      /** 将管理sk用到的多组条件解析出来 **/
         var dimensionColumns_list: List[DimensionJoinCondition] = Nil
      /**循环得到多个联结条件**/
         var dimJoinConditions = dimension_column(i).get("dimJoinConditions").get.asInstanceOf[List[Map[String, String]]]
         for (i <- 0 to dimJoinConditions.length - 1) {
              dimensionColumns_list = DimensionJoinCondition(dimJoinConditions(i).get("columnPairs").get.asInstanceOf[Map[String, String]],dimJoinConditions(i).get("dimWhereClause").mkString,null,dimJoinConditions(i).get("sourceWhereClause").mkString) :: dimensionColumns_list
         }
       dimensionColumns = new DimensionColumn(dimension_column(i).get("dimensionTableName").mkString,dimensionColumns_list,dimension_column(i).get("dimensionSkColumnName").mkString) :: dimensionColumns
      }


       dimensionsNeedInFact = null
        partition = 0

    /**
      * columnsFromSource
       */
     var fact_column: Seq[Map[String, String]] = dataObj.get("factColumns").get.asInstanceOf[List[Map[String, String]]]
     for (i <- 0 to fact_column.length-1) {
        columnsFromSource = (fact_column(i).get("columnName").mkString, fact_column(i).get("expression").mkString) :: columnsFromSource
     }
  }


  override def readSource(sourceDate: String, sourceHour: String): DataFrame = {
    /** 将数据源读取出来生成list **/
     var sources: Seq[Map[String, String]] = dataObj.get("sources").get.asInstanceOf[List[Map[String, String]]]
    /**循环读取数据源并进行union**/
     var mergerDataFrame: DataFrame = sqlContext.sql(sources(0).get("extractSql").mkString).toDF()
     for(i <- 1 to sources.length-1) {
      /// union
         mergerDataFrame = mergerDataFrame.unionAll(sqlContext.sql(sources(i).get("extractSql").mkString).toDF())
    }
      mergerDataFrame
  }

  /**
    * ETL过程执行程序
    */
  override def execute(params: Params): Unit = {
    initParam(params)
    super.execute(params)
  }
}
