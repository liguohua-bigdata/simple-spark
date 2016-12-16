package book.sparkcontext

import org.apache.spark.{SparkConf, SparkContext}

object SparkContext006 {
  def main(args: Array[String]): Unit = {
    //1.创建配置文件
    val sparkConf = new SparkConf()
    sparkConf.setAppName(this.getClass.getName)
    sparkConf.setMaster("spark://qingcheng11:7077")
    //2.创建sparkContext
    val spark = new SparkContext(sparkConf)

    //3.广播String变量
    val broadcastStr = spark.broadcast("hello spark")
    println(broadcastStr.value)

    //4.广播集合变量
    val broadcastArr = spark.broadcast(Array(1, 2, 3))
    println(broadcastArr)
    //5.广播集合变量
    val broadcastCol = spark.broadcast(1 to 9)
    println(broadcastCol.value)

    //6.关闭sparkcontext
    if (!spark.isStopped) {
      spark.stop()
    }
  }
}
