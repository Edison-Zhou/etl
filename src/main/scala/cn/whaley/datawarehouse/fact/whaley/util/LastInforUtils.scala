package cn.whaley.datawarehouse.fact.whaley.util

/**
 * Created by zhangyu on 18/4/12.
 */
object LastInforUtils {


  //计算导致播放的算法类型
  // (退出推荐>详情页>筛选站点树>其他站点树>频道首页>launcher首页)
  def getLastAlgorithmTypeFromPlayVod(playexitAlg: String, detailAlg: String, filterAlg: String,
                                      sitetreeAlg: String, channelhomeAlg: String, launcherAlg: String): String = {
    getLastAlgInfor(playexitAlg, detailAlg, filterAlg, sitetreeAlg, channelhomeAlg, launcherAlg,"TYPE")
  }

  def getLastAlgFromPlayVod(playexitAlg: String, detailAlg: String, filterAlg: String,
                            sitetreeAlg: String, channelhomeAlg: String, launcherAlg: String): String = {
    getLastAlgInfor(playexitAlg, detailAlg, filterAlg, sitetreeAlg, channelhomeAlg, launcherAlg,"ALG")
  }

  def getLastAlgInfor(playexitAlg: String, detailAlg: String, filterAlg: String,
                      sitetreeAlg: String, channelhomeAlg: String, launcherAlg: String,
                      selectType: String): String = {

    var res = Tuple2("","")
    if (playexitAlg != null && playexitAlg != "null" && playexitAlg != "none") res = ("playexit_recommend", playexitAlg)
    else if (detailAlg != null && detailAlg != "null" && detailAlg != "none") res = ("detail_recommend", detailAlg)
    else if (filterAlg != null && filterAlg != "null" && filterAlg != "none") res = ("retrieval", filterAlg)
    else if (sitetreeAlg != null && sitetreeAlg != "null" && sitetreeAlg != "none") res = ("recommend_sitetree", sitetreeAlg)
    else if (channelhomeAlg != null && channelhomeAlg != "null" && channelhomeAlg != "none") res = ("channelhome", channelhomeAlg)
    else if (launcherAlg != null && launcherAlg != "null" && launcherAlg != "none") res = ("launcher", launcherAlg)
    else ("null","null")

    selectType match {
      case "TYPE" => res._1
      case "ALG" => res._2
      case _ => null
    }

  }


  //计算最后导致播放时的datasource

  def getLastDatasource(channelhomeDataSource:String,launcherDataSource:String):String = {
    if(channelhomeDataSource != null && channelhomeDataSource != "none" && channelhomeDataSource != "null"){
      channelhomeDataSource
    }else if(launcherDataSource != null && launcherDataSource != "none" && launcherDataSource != "null"){
      launcherDataSource
    }else null
  }


}
