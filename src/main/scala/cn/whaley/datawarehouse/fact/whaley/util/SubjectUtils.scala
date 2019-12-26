package cn.whaley.datawarehouse.fact.whaley.util


/**
  * Created by guohao on 2017/5/12.
  * 收集所有关于专题的工具类到此类中
  */
object SubjectUtils {
  //专题code解析
  def getSubjectCode(path:String): String ={
    val reg = "-([a-zA-Z]+)([0-9]+)$".r
    if(path != null && path != ""){
      val format = reg findFirstMatchIn path
      val result = format match {
        case Some(x) =>{
          val videoType = x.group(1)
          val subjectCode = videoType+x.group(2)
          subjectCode
        }
        case None => null
      }
      result
    }else null
  }

  def main(args: Array[String]): Unit = {
    val paths = Array("home-tv-tv_kangzhanfengyun","home-movie-movie_zhuanti-movie887","home-tv-tv_genbo-tv524")
    paths.foreach(path=>{
      println(getSubjectCode(path))
    })

  }
}
