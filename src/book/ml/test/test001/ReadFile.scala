package ml.test.test001

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object ReadFile {
  def main(args: Array[String]): Unit = {
    //屏蔽不必要的日志显示在终端上
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.apache.eclipse.jetty.server").setLevel(Level.OFF)
    //设置运行环境
    val sparkConf = new SparkConf()
    sparkConf.setAppName("MovieLensALS").setMaster("local[5]")
    val sc = new SparkContext(sparkConf)

    val lines = sc.textFile("file:///Users/liguohua/Documents/F/code/idea/git/simple-spark/ca001.data/ml-100k/u.ca001.data")
    val string =lines.collect()
    string.foreach(print(_))
  }
}
