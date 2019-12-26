package cn.whaley.datawarehouse.fact.moretv.FactMerge

import cn.whaley.datawarehouse.global.LogConfig
import cn.whaley.datawarehouse.fact.constant.UDFConstantDimension._
import cn.whaley.datawarehouse.fact.moretv.util.Path4XParserUtils.getIndexPath
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

/**
  * Created by yang.qizhen on 2018/11/08.
  * 3x和4x的腾讯会员购买入口兼容
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
  * （1）3x实际日志打点中，当entrance为首页会员入口时，path都是null，极个别几条日志有路径，故拿不到首页区域信息
  * （2）3x频道首页会员入口，只打了游戏首页的点
  * （3）3x详情页上的各个广告，无法得知在详情页的哪个位置，因为路径只到详情页的上一级
  */
object TencentMemberEntranceUtils extends LogConfig {

  //实际日志打点中，当首页会员入口时，path都是null，极个别几条日志有路径，故拿不到首页区域信息

  def getMemberEntrance3x(entrance:String,path:String,adType:String):String ={
    var result:String = null

    val adTypeName:String = adType match {
      case "hot_information_flow" => "短视频信息流广告"
      case "tvb_little_window_play" => "TVB专区小窗播放前贴广告"
      case "play_exit" => "播控退出广告"
      case "open_screen" => "开屏广告"  //有投放
      case "interest" => "奇趣首页广告"
      case "launcher" => "首页广告"
      case "content_list" => "内容列表广告"
      case "detail_activity" => "详情页活动广告" //有投放，路径上没有详情页信息，到节目的所在页面就截止
      case "banner" => "详情页通栏广告" //有投放，路径上没有详情页信息，到节目的所在页面就截止
      case _ => null
    }

    entrance match {
      case "userCenter" => result = "会员中心开通入口"
      case "authentication" | "detailWindowAuth" => result = "节目鉴权"
      case "msgReminder" => result = "消息中心"
      //实际日志打点中，当时首页会员入口时，path都是null，极个别几条日志有路径，故拿不到首页区域信息
      case "launcherRecommend" => result = "3x首页推荐位"
      case "launcherMemberArea" => result = "首页会员看看"
      case "launcherFloatingLayer" => result = "遮罩浮层"
      case "launcherPurchase" => result = "首页开通会员入口"
      //3x频道首页会员入口，只打了游戏首页的点
      case "recommend_game" => result = "3x游戏页面"
      case "H5" => "h5"
      case "memberChannelPurchaseButton" => result = "3x会员频道购买按钮"
      //3x详情页上的各个广告，无法得知在详情页的哪个位置，因为路径只到详情页的上一级
      case "ad" => result = adTypeName  //有三个值：开屏广告、详情页活动广告、详情页通栏广告
      case "detailVIPButton"  => result = "详情页购买"
      case _ => result = null
    }
    result
  }

  def getMemberEntrance4x(entrance:String,path:Seq[Row],adType:String):String = {
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

    val adTypeName:String = adType match {
      case "hot_information_flow" => "短视频信息流广告"
      case "tvb_little_window_play" => "TVB专区小窗播放前贴广告"
      case "play_exit" => "播控退出广告"
      case "open_screen" => "开屏广告"
      case "interest" => "奇趣首页广告"
      case "launcher" => "首页广告"
      case "content_list" => "内容列表广告"
      case "detail_activity" => "详情页活动广告"
      case "banner" => "详情页通栏广告"
      case _ => "不知道什么广告类型"
    }

    /** 会员中心 **/
    if(page_type == MEMBERCENTERACTIVITY){
      access_area match {
        case "MBL-0911-1" => result = "会员中心开通入口"
        case "MBL-0911-2" => result = "4x会员中心活动入口"
        case "MBL-0912-3" => result = "4x会员中心功能入口"
        case _ => result = "4x用户中心的区域值上传错误"
      }
    }
    /** 节目鉴权 **/
    else if (entrance == "authentication" || (entrance == null && (page_type == PLAYACTIVITY || page_type == GROUPSUBJECTACTIVITY || page_type == SUBJECTHOMEACTIVITY))){
      result = "节目鉴权"
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
    else if(entrance == "floating_layer"){
      result = "遮罩浮层"
    }
    /** 频道 **/
    else if(page_type == VODACTIVITY && link_value == MTVIP){
      var channelType:String = null
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
      if(channelType != null){
        result = "4x" + channelType + "*"+ page_tab
      }

    }

    /** 广告 **/
    else if(entrance == "ad"){
      //开屏广告
       if(adType == "open_screen"){
         result = adTypeName
       }
       //详情页活动广告
       else if(adType == "detail_activity" && page_type == DETAILHOMEACTIVITY){
         result = adTypeName
       }
    }
    else if(entrance == "button"){
      //详情页开通vip按钮
      if(page_type == DETAILHOMEACTIVITY){
        result = "详情页购买"
      }
    }
    else if(page_type == "WebActivity"){
       result = "h5"
    }
    result
  }

}
