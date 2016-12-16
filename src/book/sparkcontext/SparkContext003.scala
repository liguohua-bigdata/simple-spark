package book.sparkcontext

import org.apache.spark.{SparkConf, SparkContext}
object SparkContext003 {
  def main(args: Array[String]): Unit = {
    //1.创建配置文件
    val sparkConf = new SparkConf()
    sparkConf.setAppName(this.getClass.getName)
    sparkConf.setMaster("spark://qingcheng11:7077")
    //2.创建sparkContext
    val spark = new SparkContext(sparkConf)
    //指定申请Executor的数量
    spark.requestExecutors(2)
    //3.获取数据rdd
    val path = "hdfs://qingcheng12:9000/input/spark/README.md"

    val rdd = spark.textFile(path)
    //4.显示数据rdd中的内容
    rdd.collect().foreach(println(_))
    //5.关闭sparkcontext
    if (!spark.isStopped) {
      spark.stop()
    }
  }
}
