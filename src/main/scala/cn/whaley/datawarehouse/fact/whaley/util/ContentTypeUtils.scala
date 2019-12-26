package cn.whaley.datawarehouse.fact.whaley.util

import cn.whaley.datawarehouse.global.LogConfig

/**
 * Created by zhangyu on 17/5/18.
 * 存储相关的contentType的函数
 */
object ContentTypeUtils extends LogConfig{

  def getContentType(path: String, contentType: String): String = {
    if (path == null || path.isEmpty) {
      contentType
    }else {
      val tmp = path.split("-")
      if (tmp.length >= 2) {
        if(CHANNEL_LIST.contains(tmp(1))){
          tmp(1)
        }else contentType
      } else contentType
    }
  }

  def getPathContenttypeFromPlayVod(channelhomeContenttype:String,sitetreeContenttype:String,
                                     functiontreeContenttype:String):String = {

    if(channelhomeContenttype != null) channelhomeContenttype
    else if(sitetreeContenttype != null && sitetreeContenttype != "account") sitetreeContenttype
    else if(functiontreeContenttype != null && functiontreeContenttype != "account") functiontreeContenttype
    else null
  }
}
