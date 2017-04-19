package ca001

import ca001.data.CurrentPath
import graphx.common.utils.LoggerSetter
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liguohua on 2017/4/19.
  * 相关性Correlations
  * ​ Correlations，相关度量，目前Spark支持两种相关性系数：
  * 皮尔逊相关系数（pearson）
  * 斯皮尔曼等级相关系数（spearman）。
  * 相关系数是用以反映变量之间相关关系密切程度的统计指标。
  * 简单的来说就是相关系数绝对值越大（值越接近1或者-1）,
  * 当取值为0表示不相关，
  * 取值为(0~-1]表示负相关，
  * 取值为(0, 1]表示正相关。
  */
object C004 {
  def main(args: Array[String]): Unit = {
    LoggerSetter.setLoggerOff()
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("UserCF")
    val sc = new SparkContext(sparkConf)
    //萼片长度，萼片宽度，花瓣长度和花瓣宽度,所属类别
    val data = sc.textFile(CurrentPath.currentDataPath + "iris.data")

    val seriesX = data.map(_.split(",")).map(p => p(0).toDouble)
    seriesX.foreach(println)
    val seriesY = data.map(_.split(",")).map(p => p(1).toDouble)
    seriesY.foreach(println)
    val correlation_pearson: Double = Statistics.corr(seriesX, seriesY, "pearson")
    println(correlation_pearson)
    val correlation_spearman: Double = Statistics.corr(seriesX, seriesY, "spearman")
    println(correlation_spearman)
  }
}
