package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame


/**
  * Created by zhu.bingxin on 2018-04-25.
  * 猫优选商品维度表
  */
object MyxGoods extends DimensionBase {

  dimensionName = "dim_medusa_myx_goods"

  columns.skName = "myx_goods_sk"

  columns.primaryKeys = List("myx_goods_id")

  columns.allColumns = List(
    "myx_goods_id", "myx_goods_name", "myx_goods_subtitle", "source", "is_on_sale", "provider_name", "publish_time", "publish_status",
    "create_time", "status"
  )


  readSourceType = jdbc

  sourceColumnMap = Map(
    columns.primaryKeys(0) -> "gid",
    columns.allColumns(1) -> "name",
    columns.allColumns(2) -> "subtitle",
    columns.allColumns(3) -> "source",
    columns.allColumns(4) -> "is_on_sale",
    columns.allColumns(5) -> "provider_name",
    columns.allColumns(6) -> "cast(publish_time as timestamp)",
    columns.allColumns(7) -> "publish_status",
    columns.allColumns(8) -> "cast(create_time as timestamp)",
    columns.allColumns(9) -> "status"
  )

  //sourceDb = MysqlDB.medusaVenus("myx_goods", "id", 1, 120, 1)

  //sourceFilterWhere = "status = 1"

  sourceTimeCol = "publish_time"


  override def readSource(readSourceType: SourceType.Value): DataFrame = {
    val myx_goods = MysqlDB.medusaVenus("myx_goods", "id", 1, 120, 1)
    val myx_provider = MysqlDB.medusaVenus("myx_provider", "id", 1, 120, 1)
    val myx_goods_provider_rel = MysqlDB.medusaVenus("myx_goods_provider_rel", "id", 1, 120, 1)
    sqlContext.read.format("jdbc").options(myx_goods).load().filter("status = 1")
      .createOrReplaceTempView("myx_goods")
    sqlContext.read.format("jdbc").options(myx_provider).load().filter("status = 1")
      .createOrReplaceTempView("myx_provider")
    sqlContext.read.format("jdbc").options(myx_goods_provider_rel).load().filter("status = 1")
      .createOrReplaceTempView("myx_goods_provider_rel")
    val result = sqlContext.sql(
      """
        |select a.*,c.name as provider_name
        |from myx_goods a join myx_goods_provider_rel b
        |on a.id = b.goods_id join myx_provider c
        |on b.provider_id = c.id
      """.stripMargin)
    //商品表和业务方表通过myx_goods_provider_rel关联起来

    result
  }

  fullUpdate = true

}
