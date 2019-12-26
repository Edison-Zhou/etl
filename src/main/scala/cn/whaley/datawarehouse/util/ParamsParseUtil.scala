package cn.whaley.datawarehouse.util

import scopt.OptionParser

/**
  * Created by baozhiwang on 2017/3/6.
  */
object ParamsParseUtil {

  private val default = Params()

  private val readFormat = DateFormatUtils.readFormat

  def parse(args: Seq[String], default: Params = default): Option[Params] = {
    if (args.nonEmpty) {
      val parser = new OptionParser[Params]("ParamsParse") {
        head("ParamsParse", "1.2")
        opt[Boolean]("isOnline").action((x, c) => c.copy(isOnline = x))
        opt[Boolean]("debug").action((x, c) => c.copy(debug = x))
        opt[String]("mode").action((x, c) => c.copy(mode = x))
        opt[Boolean]("isRefresh").action((x, c) => c.copy(isRefresh = x))
        opt[String]("factId").action((x, c) => c.copy(factId = x))
        opt[String]("jobName").action((x, c) => c.copy(jobName = x))
        opt[Seq[String]]("factColumn").action((x, c) => c.copy(factColumn = x))
        opt[String]("date").action((x, c) => c.copy(startDate = x)).
          validate(e => try {
            readFormat.parse(e)
            success
          } catch {
            case e: Exception => failure("wrong date format, should be 'yyyyMMdd'")
          })
        opt[String]("hour").action((x, c) => c.copy(startHour = x)).
          validate(e => try {
            if (e.length == 2 && e.toInt >= 0 && e.toInt <= 23) {
              success
            } else {
              failure("wrong date format")
            }
          } catch {
            case e: Exception => failure("wrong date format, should be 'HH'")
          })

      }
      parser.parse(args, default) match {
        case Some(p) => Some(p)
        case None => throw new RuntimeException("parse error")
      }
    } else {
      throw new RuntimeException("args is empty,at least need --isOnline")
    }
  }

}
