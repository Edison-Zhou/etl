package cn.whaley.datawarehouse.fact.moretv.FactMerge

import cn.whaley.datawarehouse.fact.constant.UDFConstantDimension._
import cn.whaley.datawarehouse.global.LogConfig
import org.apache.spark.sql.Row

import scala.util.control.Breaks.{break, breakable}

/**
  * Created by yang.qizhen on 2018/11/08.
  * 4x的自有付费会员购买入口路径解析
  *
  * 均有：
  * 会员中心开通入口（会员中心）
  * 我的“开通VIP”（首页开通会员入口）
  * 首页推荐位、会员看看推荐位（table_code）
  * 消息中心
  * 遮罩浮层
  * 开屏广告（开屏直接推会员购买）
  * 详情页广告
  * 详情页购买
  * 节目鉴权
  * 频道首页
  * h5
  *
  * 4x独有：
  * 会员中心活动入口（会员中心）
  * 大首页左侧tab开通vip（我的开通会员tab）
  *
  * 注意：
  * （1）没打entrance（腾讯日志里区分：浮层、鉴权、广告、按钮） 和 adType,
  *      故，无法识别浮层、各类型广告和按钮；
  * （2）鉴权通过is_authentication 区分；
  * （3）前贴片广告通过is_from_vod_pre_play_ad识别；
  */
object OwnMemberEntranceUtils extends LogConfig {

  def getMemberEntrance4x(path:Seq[Row],is_authentication:java.lang.Long,is_from_vod_pre_play_ad:java.lang.Long):String = {
    var result: String = null

    //获取path中最后一个页面,最后一个页面即会员购买入口所在页面
    var entrancePage: Row = null
    if(path != null) {
      entrancePage = path.reverse(0)
    }
    //获取最后一个页面page_type
    var page_type: String = null
    if(path != null && entrancePage.schema.exists(_.name == "page_type")) {
        page_type = entrancePage.getAs[String]("page_type")
    }
    //获取最后一个页面access_area
    var access_area:String = null
    if(path != null && entrancePage.schema.exists(_.name == "access_area")){
      access_area = entrancePage.getAs[String]("access_area")
    }

    val laun_access_area_name = access_area match{
      case "editor_Live" => "Live"
      case "editor_classification" => "分类"
      case "editor_funny" => "趣玩短片"
      case "editor_my" => "我的"
      case "editor_plan" => "独家策划"
      case "editor_recommend" => "今日推荐"
      case "editor_vip" => "会员看看"
      case "editor_vipbutton" => "会员"
      case "editor_search" => "搜索"
      case _ => "noExist"
    }

    //获取最后一个页面link_value
    var link_value:String = null
    if(path != null && entrancePage.schema.exists(_.name == "link_value")){
       link_value  = entrancePage.getAs[String]("link_value")
    }

    //获取最后一个页面location_index
    var location_index:String = null
    if(path != null && entrancePage.schema.exists(_.name == "location_index")){
      location_index  = entrancePage.getAs[String]("location_index")
    }

    /** 节目鉴权 **/
    if (is_authentication == 1){
      result = "节目鉴权"
    }

    /** 会员中心 **/
    else if(page_type == MEMBERCENTERACTIVITY){
      access_area match {
        case "MBL-0911-1" => result = "会员中心开通入口"
        case "MBL-0911-2" => result = "4x会员中心活动入口"
        case "MBL-0912-3" => result = "4x会员中心功能入口"
        case _ => result = "4x用户中心的区域值上传错误"
      }
    }

    /** 消息中心 **/
    else if(page_type == MESSAGECENTERACTIVITY){
      result = "消息中心"
    }
    /** 首页table_code **/
    else if(link_value == MTVIP && page_type == LAUNCHERACTIVITY){
      laun_access_area_name match {
        case "我的" => result = "首页开通会员入口"
        case "noExist" => result = "4x首页左上角开通会员tab"
        case "会员看看" => result = "首页会员看看"
        case _ => result = "4x首页" + laun_access_area_name
      }
    }
    /** 遮罩浮层 **/
//    else if(entrance == "floating_layer"){
//      result = "遮罩浮层"
//    }
    /** 频道 **/
    else if(page_type == VODACTIVITY && link_value == MTVIP) {
      var channelType: String = null
      breakable {
        entrancePage.getAs[String](PAGE_URL).split("&").foreach(url => {
          val kvs = url.split("=")
          if (kvs(0) == "channelType" && kvs.length > 1) { // 获取url中的channelType值
            channelType = kvs(1)
            break()
          }
        })
      }
      val page_tab: String = entrancePage.getAs[String](PAGE_TAB)
      if (channelType != null) {
        result = "4x" + channelType + "*" + page_tab
      }
    }
    /** 广告 **/
//    else if(entrance == "ad"){
//      //开屏广告
//       if(adType == "open_screen"){
//         result = adTypeName
//       }
//       //详情页活动广告
//       else if(adType == "detail_activity" && page_type == DETAILHOMEACTIVITY){
//         result = adTypeName
//       }
//    }

   // 详情页活动广告
    else if(page_type == DETAILHOMEACTIVITY &&  access_area == "banner" && location_index >= "1" && location_index <= "4"){
      result = "详情页活动广告"
    }
    else if(is_from_vod_pre_play_ad == 1){
      result = "前贴片广告"
    }
//    else if(entrance == "button"){
//      //详情页开通vip按钮
//      if(page_type == DETAILHOMEACTIVITY){
//        result = "详情页购买"
//      }
//    }


    else if(page_type == DETAILHOMEACTIVITY && link_value != null && (link_value.contains("购买") || link_value.contains("续费"))){
      result = "详情页购买"
    }

    else if(page_type == "WebActivity"){
       result = "h5"
    }
    result
  }

}
