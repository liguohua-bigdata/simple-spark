package book.sparkcontext

import org.apache.spark.{SparkConf, SparkContext}

object SparkContext004 {
  def main(args: Array[String]): Unit = {
    //1.创建配置文件
    val sparkConf = new SparkConf()
    sparkConf.setAppName(this.getClass.getName)
    sparkConf.setMaster("spark://qingcheng11:7077")
    //2.创建sparkContext
    val spark = new SparkContext(sparkConf)
    //3.创建空的rdd
    val rdd0=spark.emptyRDD
    rdd0.collect().foreach(println(_))

    //2/基Seq创建RDD,3个partition
    val rdd1=spark.parallelize(1 to 9,3)
    rdd1.collect().foreach(println(_))

    //3.基于hdfs文件系统创建rdd,3个partition
    val path3 = "hdfs://qingcheng12:9000/input/spark/README.md"
    val rdd3 = spark.textFile(path3,3)
    rdd3.collect().foreach(println(_))

    //4.基于本地文件系统创建rdd,3个partition
    val path4 = "$SPARK_HOME/README.md"
    val rdd4 = spark.textFile(path4,3)
    rdd4.collect().foreach(println(_))

    //5.关闭sparkcontext
    if (!spark.isStopped) {
      spark.stop()
    }
  }
}
