package book.sparkcontext

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by liguohua on 16/12/2016.
  */
object SparkContext001 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("goood")
    sparkConf.setMaster("spark://qingcheng11:7077")
    val spark = new SparkContext(sparkConf)
    val rdd=spark.textFile("hdfs://qingcheng12:9000/input/spark/README.md")
    rdd.collect().foreach(println(_))
    print(spark.version)
    print(spark.getConf)
    spark.stop()
  }

}
