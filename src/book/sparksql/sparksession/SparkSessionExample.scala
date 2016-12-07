package book.sparksql.sparksession

import org.apache.spark.sql.SparkSession

/**
  * Created by liguohua on 07/12/2016.
  */
object SparkSessionExample {
  def main(args: Array[String]) {
    //1.创建SparkSession
    val spark = SparkSession.builder.
      master("spark://qingcheng11:7077,qingcheng12:7077")
      .appName("spark-session-example")
      .enableHiveSupport()
      .getOrCreate()
    //2.读取hdfs上的文件
    val df = spark.read.option("header", "true").csv("hdfs://qingcheng12:9000/input/spark/sales.csv")
    //3.显示文件内容
    df.show()
    //4.关闭SparkSession
    spark.stop()
  }
}
