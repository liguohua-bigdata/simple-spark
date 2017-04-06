package graphx.common.utils

import org.apache.log4j.{Level, Logger}

/**
  * Created by liguohua on 24/03/2017.
  */
object LoggerSetter {
  def  setLoggerOff(): Unit ={
    //屏蔽日志
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  }

}
