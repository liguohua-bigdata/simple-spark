package ca001

import ca001.data.CurrentPath
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liguohua on 2017/4/19.
  */
object C003 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("UserCF")
    val sc = new SparkContext(sparkConf)
    //萼片长度，萼片宽度，花瓣长度和花瓣宽度,所属类别
    val observations = sc.textFile(CurrentPath.currentDataPath + "iris.data").map(_.split(",")).map(p => Vectors.dense(p(0).toDouble, p(1).toDouble, p(2).toDouble, p(3).toDouble))
    observations.foreach(println(_))
    //统计信息C003$
    val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
    println(summary.count)
    println(summary.mean)
    println(summary.max)
    println(summary.min)
    println(summary.variance)
    println(summary.normL1)
    println(summary.normL2)
    println(summary.numNonzeros)
  }
}
