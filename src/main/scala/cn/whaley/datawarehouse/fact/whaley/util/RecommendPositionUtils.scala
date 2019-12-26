package cn.whaley.datawarehouse.fact.whaley.util

/**
  * Created by huanghu 2017/5/16.
  * 收集所有关于推荐位的信息工具类到此类中(不包含首页推荐的信息）
  */
object RecommendPositionUtils {

  def getRecommendPosition(path: String, subPath: String): String = {

    if (subPath == "" || subPath == null) {
      if (path == "home-movie-movie") "guessyoulike"
      else null
    } else if (subPath == "guessyoulike") "peoplealsolike"
    else subPath
  }

  def getRecommendIndex(locationIndex: String, subPath: String): Int = {

    if (locationIndex == "" || locationIndex == null) -1
    else if (subPath == "similar" && locationIndex.toInt > 72) -2
    else if (subPath == "peoplealsolike" && locationIndex.toInt > 5) -2
    else if (subPath == "guessyoulike" && locationIndex.toInt > 100) -2
    else locationIndex.toInt + 1

  }

}


