package cn.whaley.datawarehouse.fact.whaley.util

/**
 * Created by zhangyu on 17/5/18.
 * 解析音乐频道歌手/榜单/电台维度(电台和歌手合并解析)
 */
object SingerRankRadioUtils {

  def getRadioSingerFromPath(path:String,contentType:String,omnibusSid:String):String = {
    val channel = ContentTypeUtils.getContentType(path,contentType)
    if(path == null || path.isEmpty){
      null
    }else{
      val tmp = path.split("-")
      if(tmp.length >= 4){
        channel match {
          case "mv" => {
            val subpath = tmp(2)
            subpath match {
              case "recommend" => {
                if(omnibusSid == null || omnibusSid.isEmpty) tmp(3) else null
              }
              case "site_hotsinger" => tmp(3)
              case _ => null
            }
          }
          case _ => null
        }
      }else null
    }
  }

  def getRankFromPath(path:String,contentType:String):String = {
    val channel = ContentTypeUtils.getContentType(path,contentType)
    if(path == null || path.isEmpty){
      null
    }else {
      val tmp = path.split("-")
      if(tmp.length >= 3){
        channel match {
          case "mv" => {
            tmp(2) match {
              case "rank" => {
                if(tmp.length >= 4){
                  tmp(3) match {
                    case "site_mvtop" => {
                      if(tmp.length > 4) tmp(4) else null
                    }
                    case _ => null
                  }
                }else tmp(3)
              }
              case _ => null
            }
          }
          case _ => null
        }
      }else null
    }

  }

}
