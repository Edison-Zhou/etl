package cn.whaley.datawarehouse.fact.whaley.util

/**
  * Created by huanghu 2017/5/15.
  * 收集所有关于筛选的信息工具类到此类中
  */
object RetrievalUtils {
  //筛选
  def getSortType(retrieval: String) = {

    if(retrieval != null){
      val totalInformation = retrieval.split("-")
      totalInformation(0)
    }else null
  }

  def getFilterCategoryFirst(retrieval: String) = {

    if(retrieval != null){
      val totalInformation = retrieval.split("-")

      totalInformation(1)

    }else null

  }

  def getFilterCategorySecond(retrieval: String) = {

    if(retrieval != null){
      val totalInformation = retrieval.split("-")

      totalInformation(2)

    }else null

  }

  def getFilterCategoryThird(retrieval: String) = {

    if(retrieval != null){
      val totalInformation = retrieval.split("-")
      if(totalInformation.length == 4)
      totalInformation(3)
      else if (totalInformation.length == 5)
        totalInformation(3) + "-" + totalInformation(4)

    }else null

  }

  /**
   * 求解vod20中的筛选条件,保持和1.0版本格式一致
   * @param sortType
   * @param condition
   * @return
   */
  def getFilterConditionFromPlayVod(sortType:String,condition:String) :String = {

    if(sortType != null && condition != null){
      val conditionList = condition.split(",").mkString("-")
      sortType + "-" + conditionList
    }else null
  }


}


