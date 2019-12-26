package cn.whaley.datawarehouse.fact.moretv.util

import org.apache.avro.TestAnnotation

/**
  * Created by yang.qizhen on 2017/11/07.
  * 游戏路径示例如下：
  * 三级分类示例：
  * home*classification*game-game*lp_gaming_site*3-game*我的世界
  * home*classification*game-game*lp_gaming_site*6-game*综合网游
  * home*classification*hot-hot*分类入口*5-hot*科技前沿*电台
  * 四级分类示例：
  * home*my_tv*game-game*lp_gaming_site*1-game*special*wzry*热门推荐
  */

object GamePathParseUtils {

  private val GAME_THREE_CATEGORY_REGEX = (".*(game-game)\\*(lp_gaming_site)\\*(.*[0-9])-(.*[A-Za-z])\\*(.*[\\u4e00-\\u9fa5])").r
  private val GAME_FOUR_CATEGORY_REGEX = (".*(game-game)\\*(lp_gaming_site)\\*(.*[0-9])-(.*[A-Za-z])\\*(special)\\*(.*[A-Za-z])\\*(.*[\\u4e00-\\u9fa5])").r

  def pathMainParse(path: String, out_index: Int) = {
    var res: String = null
    if (path != null && path != "") {
      if (path.contains("game-game")) {
        if (out_index == 1) {
          res = "game"
        } else if (path.contains("special")) {
          val (area, category, tab) = parse4xListCategoryInfo(path)
          if (out_index == 2) res = area else if (out_index == 3) res = category else if (out_index == 4) res = tab
        }
        else if (path.contains("-game*")) {
          val (area, category) = parse3xListCategoryInfo(path)
          if (out_index == 2) res = area else if (out_index == 3) res = category
        }
      }
    }
    res
  }

  def parse3xListCategoryInfo(str: String) = {
    var res1: String = null
    var res2: String = null

    /** Step 1: 三级站点树 */
    GAME_THREE_CATEGORY_REGEX findFirstMatchIn str match {
      case Some(p) => {
        res1 = "moreGame"
        if (p.group(5).contains("*")) {
          res2 = p.group(5).split("[*]")(0)
        }
        else {
          res2 = p.group(5)
        }
      }
      case None =>
    }
    (res1, res2)
  }

  def parse4xListCategoryInfo(str: String) = {
    var res1: String = null
    var res2: String = null
    var res3: String = null

    /** Step 1: 四级站点树 */
    GAME_FOUR_CATEGORY_REGEX findFirstMatchIn str match {
      case Some(p) => {
        res1 = "gameArea"
        res2 = p.group(6)
        res3 = p.group(7)
      }
      case None =>
    }
    (res1, res2, res3)
  }


  @TestAnnotation
  def main(args: Array[String]): Unit = {
    val str = "home*classification*game-game*lp_gaming_site*6-game*综合网游"
    println(pathMainParse(str, 1))
  }


}
