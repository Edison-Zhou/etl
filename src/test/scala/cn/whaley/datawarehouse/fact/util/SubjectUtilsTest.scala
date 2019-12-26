package cn.whaley.datawarehouse.fact.util

import cn.whaley.datawarehouse.fact.moretv.util.{Path4XParserUtils, SubjectUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.junit.{Before, Test}

class SubjectUtilsTest {

  var path_4x: Seq[Row] = null

  val conf = new SparkConf().setMaster("local[*]").setAppName("test")

  var spark: SparkSession = null
  var sc: SparkContext = null
  implicit var sqlContext: SQLContext = null

  @Before
  def init() = {
    println("start init ....")

//    spark = SparkSession.builder()
//      .config(conf)
      //.enableHiveSupport()
   //   .getOrCreate()

    println("end init ....")

//    val rdd = spark.sparkContext.textFile("file:///D:\\IEDAWorkspace\\DataWarehouseEtlSpark\\src\\test\\scala\\medusa_path_test.log")

//    path_4x = spark.read.json(rdd)
//        .collect()(3)
//        .getAs[Seq[Row]](0)


  }

  @Test
  def getInfo() = {
//    println(SubjectUtils.getSubjectEntranceByPath4xETL(path_4x))
 //   println(Path4XParserUtils.getChannelPlayEntrance(path_4x))
  }


}
