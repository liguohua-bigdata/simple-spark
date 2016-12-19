package book.sparkconfig

import org.apache.spark.{SparkConf, SparkContext}

object SparkConfig001 {
  def main(args: Array[String]): Unit = {
    //1.创建配置文件
    val sparkConf = new SparkConf()
    sparkConf.setAppName("testSparkConf")
    sparkConf.set("spark.logConf","true")
    sparkConf.setMaster("spark://qingcheng11:7077")
    //2.创建sparkContext
    val spark = new SparkContext(sparkConf)
    //3.获取数据rdd
    val rdd = spark.textFile("hdfs://qingcheng11:9000/input/spark/README.md")
    //4.显示数据rdd中的内容
    rdd.collect().foreach(println(_))
    //5.关闭sparkcontext
    if (!spark.isStopped) {
      spark.stop()
    }
  }
}
