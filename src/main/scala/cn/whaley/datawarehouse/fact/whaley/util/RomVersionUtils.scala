package cn.whaley.datawarehouse.fact.whaley.util

/**
 * Created by zhangyu on 17/5/16.
 * 获取微鲸电视的romVersion版本
 */
object RomVersionUtils {

  /**
   * 获取电视机的romVersion版本(WUI版本)
   * @param romVersion
   * @param firmwareVersion
   * @return
   */
  def getRomVersion(romVersion:String,firmwareVersion:String):String = {

    if(romVersion == null || romVersion.isEmpty){
      getRomVersionFromFirmwareVersion(firmwareVersion)
    }else{
      if(romVersion.contains("-")){
        getRomVersionFromFirmwareVersion(romVersion)
      }else romVersion
    }
  }

  /**
   * 从固件版本中提取romVersion版本(WUI版本)
   * @param firmwareVersion
   * @return
   */
  def getRomVersionFromFirmwareVersion(firmwareVersion:String):String = {
    if(firmwareVersion == null || firmwareVersion.isEmpty){
      null
    }else{
      val startIndex = firmwareVersion.indexOf("-")
      val endIndex = firmwareVersion.lastIndexOf(".")
      if(startIndex > -1 && endIndex > startIndex){
        val rom = firmwareVersion.substring(startIndex+1,endIndex)
        rom
      }else null
    }
  }

}
