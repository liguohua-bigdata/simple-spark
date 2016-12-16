package book.sparkcontext

import org.apache.spark.{SparkConf, SparkContext, SparkFiles}

object SparkContext002 {
  def main(args: Array[String]): Unit = {
    //1.创建配置文件
    val sparkConf = new SparkConf()
    sparkConf.setAppName("isStopped和stop")
    sparkConf.setMaster("spark://qingcheng11:7077")
    //2.创建sparkContext
    val spark = new SparkContext(sparkConf)
    val path="$SPARK_HOME/README.md"
    spark.addFile(path)
    //3.获取数据rdd
    val rdd=spark.textFile(SparkFiles.get(path))
    //4.显示数据rdd中的内容
    rdd.collect().foreach(println(_))
    //5.关闭sparkcontext
    if (!spark.isStopped) {
      spark.stop()
    }
  }
}
