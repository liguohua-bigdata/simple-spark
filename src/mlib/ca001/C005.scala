package ca001

import graphx.common.utils.LoggerSetter
import org.apache.spark.{SparkConf, SparkContext}

/**
  * ​分层取样（Stratified sampling）顾名思义，
  * 就是将数据根据不同的特征分成不同的组，
  * 然后按特定条件从不同的组中获取样本，并重新组成新的数组。
  * 分层取样算法是直接集成到键值对类型
  * RDD[(K, V)] 的 sampleByKey 和 sampleByKeyExact 方法，
  * 无需通过额外的 spark.mllib 库来支持。
  */
object C005 {
  def main(args: Array[String]): Unit = {
    LoggerSetter.setLoggerOff()
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("UserCF")
    val sc = new SparkContext(sparkConf)
    val data = sc.makeRDD(Array(
      ("female", "Lily"),
      ("female", "Lucy"),
      ("female", "Emily"),
      ("female", "Kate"),
      ("female", "Alice"),
      ("male", "Tom"),
      ("male", "Roy"),
      ("male", "David"),
      ("male", "Frank"),
      ("male", "Jack")))
    val fractions: Map[String, Double] = Map(
      "female" -> 0.6,
      "male" -> 0.4)
    //效率高，准确性低
    val approxSample = data.sampleByKey(withReplacement = false, fractions, 1)
    approxSample.foreach(println)

    println("*************************************************")
    //效率低，准确性高
    val exactSample = data.sampleByKeyExact(withReplacement = false, fractions, 1)
    exactSample.foreach(println)

  }
}
