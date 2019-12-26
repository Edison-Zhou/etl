package cn.whaley.datawarehouse.market.dim.medusa

import cn.whaley.datawarehouse.global.SourceType
import cn.whaley.datawarehouse.market.dim.DmDimensionBase
import org.apache.spark.sql.DataFrame
import cn.whaley.datawarehouse.global.SourceType._

/**
  * 集市层维度表dw_dm.dim_medusa_terminal_user
  * Created by yang.qizhen on 2019/9/10 14:13
  */
object TerminalUser extends DmDimensionBase{

  columns.skName="terminal_user_sk"
  columns.primaryKeys = List("userId")
  columns.trackingColumns = List()
  columns.allColumns = List(
    "userId",
    "version",
    "build_time",
    "product_model",
    "brand_name",
    "open_time",
    "user_type",
    "country",
    "province",
    "city",
    "city_level",
    "dangerous_level",
    "ip_section_123",
    "promotion_channel"
  )

  readSourceType = hive

  dimensionName = "dim_medusa_terminal_user"

  fullUpdate = true

  /**
    * 自定义的源数据读取方法
    * 默认是从jdbc中读取，如需自定义，可以在子类中重载实现
    *
    * @return
    */
  override def readSource(readSourceType: SourceType.Value): DataFrame = {

    /**
      * 每个表来源维度表
      * version,build_time ==>dw_dimensions.dim_app_version
      * brand_name ==>dw_dimensions.dim_medusa_product_model
      * user_id,mac,open_time,user_type,product_model,promotion_channel ==>dw_dimensions.dim_medusa_terminal_user
      * province,city,city_level,dangerous_level,ip_section_123 ==>dw_dimensions.dim_web_location
      */
    //所有维度字段信息提取
    sqlContext.sql(
      """
        | select e.user_id as userId,e.open_time,e.user_type,e.product_model,e.brand_name,
        |        e.promotion_channel,f.country,f.province,f.city,f.city_level,f.dangerous_level,
        |        f.ip_section_123,e.version,e.build_time
        | from
        | (
        |    select c.user_id,c.open_time,c.user_type,c.product_model,d.brand_name,
        |           c.promotion_channel,c.web_location_sk,c.version,c.build_time
        |    from
        |    (
        |      select a.user_id,a.open_time,a.user_type,a.product_model,a.promotion_channel,
        |               a.web_location_sk,b.version,b.build_time
        |      from
        |      (
        |        select distinct user_id,open_time,user_type,product_model,promotion_channel,
        |               web_location_sk,app_version_sk
        |        from dw_dimensions.dim_medusa_terminal_user
        |        where dim_invalid_time is null
        |              and user_id is not null
        |       ) a
        |      left outer join
        |      (
        |        select version,build_time,app_version_sk
        |        from dw_dimensions.dim_app_version
        |        where dim_invalid_time is null
        |      ) b
        |      on a.app_version_sk = b.app_version_sk
        |    ) c
        |    left outer join
        |    (
        |      select product_model,brand_name
        |      from dw_dimensions.dim_medusa_product_model
        |      where dim_invalid_time is null
        |    ) d
        |    on c.product_model = d.product_model
        |  ) e
        | left outer join
        | (
        |    select country,province,city,city_level,dangerous_level,ip_section_123,web_location_sk
        |    from dw_dimensions.dim_web_location
        |    where dim_invalid_time is null
        |  ) f
        |  on e.web_location_sk = f.web_location_sk
      """.stripMargin)
  }
}
