package cn.whaley.datawarehouse.fact.whaley.util

/**
  * 创建人：郭浩
  * 创建时间：2017/5/15
  * 程序作用：站点树解析
  * 数据输入：
  * 数据输出：
  */
object ListCategoryUtils {
  /**
    * 获取站点树
    * @param path
    *   @return 最后一个站点树
    */
  def getLastFirstCode(path:String):String ={
    getListCategoryCode(path)(2)
  }

  /**
    * @param path
    * @return mv,sports,kids 倒数第二个站点树,其他取contentType
    */
  def getLastSecondCode(path:String):String ={
    getListCategoryCode(path)(1)
  }

  /**
    * 获取contentType
    * @param path
    * @return
    */
  def contentType(path:String):String={
    getListCategoryCode(path)(0)
  }

  /**
    *
    * @param path
    * @return contentType:倒数第二个站点树:最后一个站点树
    */
  def getListCategoryCode(path:String):Array[String] ={
    val categoryCodes= new Array[String](3)
    if(path == null) {
      return categoryCodes
    }
    val paths = path.split("-")
    if(paths.length >2){
      val contentType = paths(1)
      categoryCodes(0)= contentType
      contentType match {
        case "sports" => {
          //sports 站点树取第四位
          if(paths.length>=3){
            categoryCodes(1) = paths(2)
            if(paths.length>=4) {
              categoryCodes(2) = paths(3)
            }else {
              categoryCodes(2) = null
            }
          }
        }
        case "mv"  =>{
          //mv 站点树取第5位
          if(paths.length>=5){
            categoryCodes(1)= paths(3)
            categoryCodes(2)= paths(4)
          }
        }
        case "kids" => {
          val kids_type=paths(2)
          kids_type match{
            case "kids_value_added_package" =>{
              // 鲸鲸学院->精选推荐 站点树取第6位
              if(paths.length>=6){
                categoryCodes(1) = paths(4)
                categoryCodes(2) = paths(5)  //需要测试
              }
            }
            //猫推荐 home-kids-recommendation
            case "recommendation" =>{
              categoryCodes(1)= paths(1)
              categoryCodes(2)="kids_scroll"
            }

            case _ =>{
              //少儿其他  站点树取第4位
                //需要处理
                kids_type match{
                  //前台code和后台code映射
                  //鲸鲸学园
                  case "wcampus" => categoryCodes(1)="kids_value_added_package"
                  //看动画片
                  case "animation" => categoryCodes(1)="show_kidsSite"
                  //听儿歌
                  case "rhyme" => categoryCodes(1)="show_kidsSongSite"
                  //学知识
                  case "learn" => categoryCodes(1)="kids_learning"
                  //少儿收藏
                  //观看历史:home-kids-collection-history_collect
                  //收藏追看:home-kids-collection-episode_collect
                  //专题收藏:home-kids-collection-subject_collect-kids73
                  case "collection" => categoryCodes(1)="kids_collect"
                  case _ =>  categoryCodes(1)= kids_type
                }
              if(paths.length >= 4){
                if(paths.length >= 5 && paths(3) == "kids" && paths(4) == "english"){
                  categoryCodes(2) = "kids-english"
                } else {
                  categoryCodes(2) = paths(3)
                }
              } else {
                categoryCodes(2) = null
              }

            }
          }
        }
        case "hot" | "interest" => {
          if(paths.length >= 4){
            categoryCodes(1)= "site_" + paths(1)
            categoryCodes(2)= paths(3)
          }else if(paths.length >= 3){
            categoryCodes(1)= contentType
            categoryCodes(2)= paths(2)
          }
        }
        case _  => {
          //其他频道 站点树取第3位
          if(paths.length>=3){
            categoryCodes(1)= contentType
            categoryCodes(2)= paths(2)
          }
        }
      }
    }
    categoryCodes
  }

/*

  def getLastFirstCode(path:String):String ={
    var lastFirstCode :String = null
    val paths = path.split("-")
    if(paths.length >2){
      val contentType = paths(1)
      contentType match {
        case "sports" => lastFirstCode ={
          //sports 站点树取第四位
          if(paths.length>=4){
            lastFirstCode= paths(3)
          }
          lastFirstCode
        }
        case "mv"  =>lastFirstCode={
          //mv 站点树取第5位

          if(paths.length>=5){
            lastFirstCode= paths(4)
          }
          lastFirstCode
        }
        case "kids" =>lastFirstCode = {
          val kids_type=paths(2)

          kids_type match{
            //鲸鲸学院->精选推荐
            case "kids_value_added_package" =>
              lastFirstCode = {
                if(paths.length>=6){
                  lastFirstCode = paths(5)
                }
                lastFirstCode
              }
            case _ =>  lastFirstCode={
              if(paths.length>=4){
                lastFirstCode= paths(3)
              }
              lastFirstCode
            }
          }
          lastFirstCode
        }
        case _  => lastFirstCode = {
          //其他频道 站点树取第3位
          if(paths.length>=3){
            lastFirstCode= paths(2)
          }
          lastFirstCode
        }
      }
    }
    lastFirstCode
  }


  /**
    * 取前一个
    * @param path
    * @return
    */
  def getLastSecondCode(path:String):String ={
    var lastSecondCode :String = null
    val paths = path.split("-")
    if(paths.length >2){
      val contentType = paths(1)
      contentType match {

        case "sports" => lastSecondCode ={
          //sports 站点树取第四位 ok
          if(paths.length>=4){
            lastSecondCode= paths(2)
          }
          lastSecondCode
        }
        case "mv"  =>lastSecondCode={
          //mv 站点树取第5位 ok
          if(paths.length>=5){
            lastSecondCode= paths(3)
          }
          lastSecondCode
        }
        case "kids" =>lastSecondCode = {
          val kids_type=paths(2)
          kids_type match{
            //鲸鲸学院->精选推荐
            case "kids_value_added_package" =>
              lastSecondCode ={
                if(paths.length>=6){
                  lastSecondCode = paths(4)
                }
                lastSecondCode
              }
            case _ =>
              lastSecondCode={
                if(paths.length>=4){
                  //需要处理
                  lastSecondCode= paths(2)
                  lastSecondCode match{
                    //鲸鲸学园
                    case "wcampus" => lastSecondCode="kids_value_added_package"
                    //看动画片
                    case "animation" => lastSecondCode="show_kidsSite"
                    //听儿歌
                    case "rhyme" => lastSecondCode="show_kidsSongSite"
                    //学知识
                    case "learn" => lastSecondCode="kids_learning"
                    case "recommendation" => lastSecondCode="kids_scroll"

                  }
                }
                lastSecondCode
              }
          }
          lastSecondCode
        }
        case _  => lastSecondCode = {
          //其他频道 站点树取第3位 ok
          if(paths.length>=3){
            lastSecondCode= contentType
          }
          lastSecondCode
        }
      }
    }
    lastSecondCode
  }
*/

  def main(args: Array[String]): Unit = {
    val paths = Array("home-kids-recommendation","home-kids-collection-subject_collect-kids73","home-movie-movie_hot-台湾-林嘉欣","home-mv-class-site_mvstyle-1_mv_style_pop",
      "home-sports-cba-league_matchreplay","home-kids-rhyme-songs_jingdian-kids212","home-kids-animation-kids_star-kids220","home-kids-kids_value_added_package-kids_jingxuanzhuanqu-kids_bbc_animation-kids_bbc_hot-movie851")
    paths.foreach(path=>{
      println(path+ " : "+ getLastSecondCode(path)+" :  "+getLastFirstCode(path) +" : "+ getListCategoryCode(path).toList )
    })
  }


  def getLastFirstCodeVod(functionTreeContenttype:String,functionTreeCode:String,
                           sitetreeContenttype:String,sitetreeCode:String,
                           functionTreePageValue:String,sitetreePageValue:String):String = {
    if(functionTreeContenttype != "account" && sitetreeContenttype == null) functionTreeCode
    else if(sitetreeContenttype != "account" && functionTreeContenttype == null)  sitetreeCode
    else null
  }

  def getLastSecondCodeVod(functionTreeContenttype:String,siteTreeContenttype:String,
                           functionTreePageValue:String,sitetreePageValue:String):String = {
    if(functionTreeContenttype != "account" && siteTreeContenttype == null) functionTreeContenttype
    else if(siteTreeContenttype != "account" && functionTreeContenttype == null){
      if(sitetreePageValue == "mv") sitetreePageValue else siteTreeContenttype
    }
    else null
  }
}
