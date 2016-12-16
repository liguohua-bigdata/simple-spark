package book.sparkcontext

import org.apache.spark.{SparkConf, SparkContext}

object SparkContext005 {
  def main(args: Array[String]): Unit = {
    //1.创建配置文件
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Accumulator")
    sparkConf.setMaster("local[*]")
    //2.创建sparkContext
    val spark = new SparkContext(sparkConf)

    val counter = spark.longAccumulator("MyCounter")

    spark.parallelize(1 to 9).foreach(n => counter.add(n))
    //5.关闭sparkcontext
    if (!spark.isStopped) {
      spark.stop()
    }
  }
}
