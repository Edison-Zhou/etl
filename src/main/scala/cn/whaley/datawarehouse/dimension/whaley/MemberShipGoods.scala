package cn.whaley.datawarehouse.dimension.whaley

import cn.whaley.datawarehouse.dimension.DimensionBase
import cn.whaley.datawarehouse.global.SourceType._
import cn.whaley.datawarehouse.util.MysqlDB
import org.apache.spark.sql.DataFrame


/**
  * Created by 郭浩 on 17/4/13.
  *
  * 微鲸会员商品
  */
object MemberShipGoods extends DimensionBase {
  columns.skName = "membership_goods_sk"
  columns.primaryKeys = List("goods_no")
  columns.trackingColumns = List()
  columns.allColumns = List("goods_no","goods_name","goods_package_type",
    "goods_tag","prime_price","discount","real_price","status","goods_type",
    "goods_title", "is_display","create_time","update_time","publish_time",
    "start_time","end_time")

  readSourceType = jdbc

  sourceDb = MysqlDB.whaleyDolphin("dolphin_whaley_goods","id",1, 1000000000, 10)

  dimensionName = "dim_whaley_membership_goods"

  sourceTimeCol = "publishTime"

  override def filterSource(sourceDf: DataFrame): DataFrame = {
    val sq = sqlContext
    import sq.implicits._
    import org.apache.spark.sql.functions._
    sourceDf.
      selectExpr(
        "goodsNo as goods_no ",
        "goodsName as  goods_name " ,
        "(case when trim(goodsName) = '白金升级钻石' then '白金升级钻石'  when  trim(goodsName) like '%鲸鲸%' then '鲸鲸会员'  when  trim(goodsName) like '%钻石%' then '钻石会员' when  trim(goodsName) like '%白金%' then '白金会员' else '其他' end ) as goods_package_type " ,
        "goodsUseTag as goods_tag " ,
        "primePrice as prime_price" ,
        "cast(discount as double ) as discount" ,
        "realPrice as real_price" ,
        "goodsStatus as status" ,
        "goodsType as goods_type" ,
        "goodsTitle as goods_title" ,
        "isDisplay as is_display" ,
        "cast(createTime as timestamp ) as create_time " ,
        "cast(updateTime as timestamp ) as update_time " ,
        "cast(publishTime as timestamp ) as publish_time " ,
        "cast(startTime as timestamp ) as start_time " ,
        "cast(endTime as timestamp ) as end_time "
      )
  }

}
