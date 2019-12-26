package cn.whaley.datawarehouse.dimension.moretv

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB


/**
  * Created by Tony on 17/3/8.
  *
  * 电视猫产品型号维度表
  */
object ProductModel extends DimensionBase {
  columns.skName = "product_model_sk"
  columns.primaryKeys = List("product_model")
  columns.trackingColumns = List()
  columns.allColumns = List("product_model", "brand_name", "product_first_type", "product_secondary_type")

  readSourceType = jdbc

  sourceColumnMap = Map(
  )

  sourceDb = MysqlDB.dwDimensionDb("moretv_product_model")

  dimensionName = "dim_medusa_product_model"

}
