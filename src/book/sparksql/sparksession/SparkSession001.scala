package book.sparksql.sparksession

import book.utils.MasterUrl
import org.apache.spark.sql.SparkSession

/**
  * Created by liguohua on 17/12/2016.
  */
object SparkSession001 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master(MasterUrl.localAll)
      .enableHiveSupport()
      .appName(this.getClass.getName)
      .getOrCreate()
    val csv = spark.read.option("header", "true").csv("hdfs://qingcheng11:9000/input/spark/sales.csv")
    csv.show()
    val json = spark.read.json("hdfs://qingcheng11:9000/input/spark/teacher.json")
    json.show()
    val text = spark.read.textFile("hdfs://qingcheng11:9000/input/spark/README.md")
    text.show()
    val text2 = spark.read.text("hdfs://qingcheng11:9000/input/spark/README.md")
    text2.show()
    val text3 = spark.read.text("hdfs://qingcheng11:9000/input/spark/person_libsvm.txt", "hdfs://qingcheng11:9000/input/spark/README.md")
    text3.show()
    val text4 = spark.read.text("hdfs://qingcheng11:9000/input/spark/*.csv", "hdfs://qingcheng11:9000/input/spar*/*.json")
    text4.show(Int.MaxValue - 1)

  }
}
