package cn.whaley.datawarehouse.fact.util

import org.junit.{Before, Test}

/**
  * Created by zhu.bingxin on 2018/5/4.
  */
class GetSubjectCodeTest {


  private val regex_etl ="""([a-zA-Z_]+)([0-9]+)""".r
  //private val regex_etl="""(movie|tv|hot|kids|zongyi|comic|jilu|sports|xiqu|mv)([0-9]+)""".r
  //private val regex_etl="""(movie|tv|hot|kids|zongyi|comic|jilu|sports|xiqu|mv|kid)([0-9]+)""".r
  private val regexSubjectName =
  """subject-([a-zA-Z0-9-\u4e00-\u9fa5]+)""".r

  // 获取专题code
  @Test
  def getSubjectCode: Unit = {

    println(getSubjectCode1("subject-魔哒解说我的世界-game_auto_33737"))
    print(getSubjectCode1("subject-新闻头条-hot11"))
  }

  def getSubjectCode1(subject: String) = {
    var subjectCode: String = null
    if (subject != null) {
      regex_etl findFirstMatchIn subject match {
        // 如果匹配成功，说明subject包含了专题code，直接返回专题code
        case Some(m) => {
          subjectCode = m.group(1) + m.group(2)
        }
        case None =>
      }
    }
    subjectCode
  }

  //  "subject-新闻头条-hot11"
}
