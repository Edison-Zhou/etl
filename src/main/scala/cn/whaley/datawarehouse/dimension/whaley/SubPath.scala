package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB

/**
  * Created by zhangyu on 17/3/14.
  * 站点树维度表
  */
object SubPath extends DimensionBase{

   columns.skName = "subpath_sk"
   columns.primaryKeys = List("subpath_id")
   columns.trackingColumns = List()
   columns.allColumns = List("subpath_id","subpath_code","subpath_name","subpath_content_type","subpath_template_code","subpath_parent_id","subpath_status","subpath_create_time","subpath_root")

   readSourceType = jdbc

   //维度表的字段对应源数据的获取方式
   sourceColumnMap = Map(
     "subpath_id" -> "id",
     "subpath_code" -> "code",
     "subpath_name" -> "name",
     "subpath_content_type" -> "contentType",
     "subpath_template_code" -> "templateCode",
     "subpath_parent_id" -> "parentId",
     "subpath_status" -> "status",
     "subpath_create_time"->"createTime",
     "subpath_root"->"root"
   )

   sourceFilterWhere = "subpath_id is not null and subpath_status = 1 and subpath_root != '0'"
   sourceDb = MysqlDB.whaleyCms("mtv_program_site","id",1,4131,10)

   dimensionName = "dim_whaley_subpath"

  fullUpdate = true

 }
