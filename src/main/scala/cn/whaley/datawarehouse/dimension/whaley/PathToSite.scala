package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB

/**
  * Created by zhangyu on 17/3/14.
  * 从日志到真实站点树的变更的维度表
  */
object PathToSite extends DimensionBase{

   columns.skName = "path_to_site_sk"
   columns.primaryKeys = List("path_to_site_id")
   columns.trackingColumns = List()
   columns.allColumns = List("path_to_site_id","path_to_site_path_code","path_to_site_program_site_code","path_to_site_content_type","path_to_site_status")

   readSourceType = jdbc

   //维度表的字段对应源数据的获取方式
   sourceColumnMap = Map(
     "path_to_site_id" -> "id",
     "path_to_site_path_code" -> "path_code",
     "path_to_site_program_site_code" -> "program_site_code",
     "path_to_site_content_type" -> "content_type",
     "path_to_site_status" -> "status"
   )

   sourceFilterWhere = "path_to_site_id is not null and path_to_site_status = 1"
   sourceDb = MysqlDB.dwDimensionDb("whaley_path_code_program_code")

   dimensionName = "dim_whaley_path_to_site"

 }
