package cn.whaley.datawarehouse.dimension.share

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.{DataExtractUtils, MysqlDB}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Created by Tony on 16/12/23.
  *
  * web地址维度生成ETL
  *
  * 无需自动触发，只需要修改后手动执行一次
  *
  * updated by wu.jiulin on 17/04/20
  * 修改city_info的表结构和数据
  * 维度表增加prefecture_level_city(地级市)字段
  */
object WebLocation extends DimensionBase {

  dimensionName = "dim_web_location"

  columns.skName = "web_location_sk"

  columns.primaryKeys = List("web_location_key")

  columns.trackingColumns = List()

  columns.allColumns = List(
    "web_location_key",
    "ip_section_1",
    "ip_section_2",
    "ip_section_3",
    "ip_section_123",
    "country",
    "area",
    "province",
    "city",
    "city_level",
    "executive_level",
    "prefecture_level_city",
    "district",
    "longitude",
    "latitude",
    "isp",
    "town_id",
    "dangerous_level"
  )


  readSourceType = custom

  fullUpdate = true

  override def readSource(readSourceType: SourceType): DataFrame = {
    val countryBlackList = List("保留地址", "骨干网")

    val ipDataRdd = sc.textFile("hdfs://hans/log/ipLocationData/ip_country.txt", 10)
      .map(r => r.split("\t")).map(r => {
      val ip = r(0).split("\\.")
      Row(ip(0).trim().toLong * 256 * 256 * 256 + ip(1).trim().toLong * 256 * 256 + ip(2).trim().toLong * 256,
        ip(0).trim().toInt, ip(1).trim().toInt, ip(2).trim().toInt,
        r(2), r(3), r(4), r(5), r(8).trim.toDouble, r(9).trim.toDouble)
    })

    val schema = new StructType(Array(
      StructField("web_location_key", LongType),
      StructField("ip_section_1", IntegerType),
      StructField("ip_section_2", IntegerType),
      StructField("ip_section_3", IntegerType),
      StructField("country", StringType),
      StructField("province", StringType),
      StructField("city", StringType),
      StructField("district", StringType),
      StructField("longitude", DoubleType),
      StructField("latitude", DoubleType)
    ))

    val ipDataDf = sqlContext.createDataFrame(ipDataRdd, schema)
    ipDataDf.createOrReplaceTempView("a")

    val schema2 = new StructType(Array(
      StructField("web_location_key", LongType),
      StructField("ip_section_1", IntegerType),
      StructField("ip_section_2", IntegerType),
      StructField("ip_section_3", IntegerType),
      StructField("country", StringType),
      StructField("province", StringType),
      StructField("city", StringType),
      StructField("district", StringType),
      StructField("isp", StringType)
    ))

    val ipDataRdd2 = sc.textFile("hdfs://hans/log/ipLocationData/mydata4vipday2.txt", 100)
      .map(r => r.split("\t")).map(r => {
      r.map(s => {
        if (s.trim.equals("*")) ""
        else s
      })
    }).filter(r => {
      var containBlack = false
      for (black <- countryBlackList) {
        if (r(2).indexOf(black) > -1) containBlack = true
      }
      !containBlack
    }).filter(r => {
      val startIp = r(0).split("\\.")
      val endIp = r(1).split("\\.")
      if (startIp.size != 4 || endIp.size != 4) false
      else if (startIp(3).equals("000") && endIp(3).equals("255")) true
      else false
    }).flatMap(r => {
      val list = collection.mutable.Buffer[(Long, Int, Int, Int, String, String, String, String, String)]()
      val startIp = r(0).split("\\.")
      val endIp = r(1).split("\\.")
      val start_key = startIp(0).trim.toLong * 256 * 256 + startIp(1).trim.toLong * 256 + startIp(2).trim.toLong
      val end_key = endIp(0).trim.toLong * 256 * 256 + endIp(1).trim.toLong * 256 + endIp(2).trim.toLong

      for (key <- start_key to end_key) {
        val sec3 = key % 256
        val sec2 = key / 256 % 256
        val sec1 = key / 256 / 256
        val row = (key * 256, sec1.toInt, sec2.toInt, sec3.toInt,
          r(2), r(3), r(4), r(5), r(6))
        list.append(row)
      }
      list.toList
    }).map(r => Row.fromTuple(r))

    val ipDataDf2 = sqlContext.createDataFrame(ipDataRdd2, schema2)
    ipDataDf2.createOrReplaceTempView("b")

    sqlContext.sql("select if(a.web_location_key is null, b.web_location_key, a.web_location_key) as web_location_key," +
      "if(a.ip_section_1 is null, b.ip_section_1, a.ip_section_1) as ip_section_1, " +
      "if(a.ip_section_2 is null, b.ip_section_2, a.ip_section_2) as ip_section_2, " +
      "if(a.ip_section_3 is null, b.ip_section_3, a.ip_section_3) as ip_section_3, " +
      "if(a.country is null or a.country = '', b.country, a.country) as country, " +
      "if(a.province is null or a.province = '', b.province, a.province) as province, " +
      "if(a.city is null or a.city = '', b.city, a.city) as city, " +
      "if(a.district is null or a.district = '', b.district, a.district) as district, " +
      " a.longitude, a.latitude, b.isp " +
      " from a full join b on a.web_location_key = b.web_location_key " +
      " order by web_location_key"
    ).select(
      "web_location_key", "ip_section_1", "ip_section_2", "ip_section_3",
      "country", "province", "city", "district", "longitude", "latitude", "isp"
    ).withColumn("city", expr("if(trim(city) = '' , null, city)")
    ).withColumn("district", expr("if(trim(district) = '' , null, district)"))

  }

  override def filterSource(sourceDf: DataFrame): DataFrame = {

    val sq = sqlContext
    sqlContext.udf.register("replaceStr", (s: String) => {
      if (s != null) s.replaceAll("市", "") else s
    })

    //    val cityInfoDb = MysqlDB.dwDimensionDb("city_info")
    //
    //    val cityInfoDf = sqlContext.read.format("jdbc").options(cityInfoDb).load()
    //      .select($"city", $"area", $"city_level")
    //
    //    cityInfoDf.join(sourceDf, "city" :: Nil, "rightouter")
    //      .select(
    //        columns.primaryKeys(0),
    //        columns.allColumns: _*
    //      )

    val cityInfoDb = MysqlDB.dwDimensionDb("city_info_new")
    val cityInfoDf = sqlContext.read.format("jdbc").options(cityInfoDb).load()

    val sourceDb = MysqlDB.medusaProgramInfo("mtv_dgcity")
    val dangerCityDf = DataExtractUtils.readFromJdbc(sqlContext, sourceDb).filter("judge = '1'")

    cityInfoDf.registerTempTable("city_info")
    sourceDf.registerTempTable("ip_info")

    var sqlStr =
      s"""
         |select
         |a.web_location_key as web_location_key,
         |a.ip_section_1 as ip_section_1,
         |a.ip_section_2 as ip_section_2,
         |a.ip_section_3 as ip_section_3,
         |a.country as country,
         |b.area as area,
         |a.province as province,
         |if(b.city is null or b.city='',a.city,b.city) as city,
         |a.district as district,
         |b.city_level as city_level,
         |a.longitude as longitude,
         |a.latitude as latitude,
         |a.isp as isp,
         |a.city as prefecture_level_city,
         |b.executive_level,
         |b.town_id
         |from ip_info a
         |left join
         |(
         |select t1.city,t1.area,t1.city_level,t1.executive_level,t1.prefecture_level_city,t1.town_id from
         |city_info t1 left join city_info t2 on t1.city=t2.prefecture_level_city
         |where t2.prefecture_level_city is null
         |) b
         |on if(b.executive_level='县级市',replaceStr(a.district),a.city) = b.city
      """.stripMargin
    sqlContext.sql(sqlStr).registerTempTable("tmp_table")
    /**
      * city_info表的city字段没有 区 的值,如浦东新区,和ip库关联不上导致最终结果有 区 的记录 area,city,city_info三个字段为空
      * 拿上面结果的prefecture_level_city关联city_info的city补上结果
      */
    sqlStr =
      s"""
         |select
         |a.web_location_key,
         |a.ip_section_1,
         |a.ip_section_2,
         |a.ip_section_3,
         |concat_ws('.',a.ip_section_1,a.ip_section_2,a.ip_section_3) as ip_section_123,
         |a.country,
         |if(a.area is null,b.area,a.area) as area,
         |a.province,
         |if(a.city is null,b.city,a.city) as city,
         |if(a.executive_level is null,b.executive_level,a.executive_level) as executive_level,
         |a.district,
         |if(a.city_level is null,b.city_level,a.city_level) as city_level,
         |a.longitude,
         |a.latitude,
         |a.isp,
         |a.prefecture_level_city,
         |if(a.town_id is null, b.town_id, a.town_id) as town_id
         |from tmp_table a left join city_info b
         |on a.prefecture_level_city=b.city
      """.stripMargin

    sqlContext.sql(sqlStr).as("t")
      .join(dangerCityDf.as("d"), expr("t.city = d.cityName"), "leftouter")
      .selectExpr("t.*", "case when d.level_city = 'a' then 2 when d.level_city = 'b' then 1 else 0 end as dangerous_level")

  }
}
