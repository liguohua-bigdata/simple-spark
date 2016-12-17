package book.sparksql.sparksession

import book.utils.MasterUrl
import org.apache.spark.sql.SparkSession

/**
  * Created by liguohua on 17/12/2016.
  */
object SparkSession001 {
  def main(args: Array[String]): Unit = {
    //0.创建SparkSession
    val spark = SparkSession.builder
      .master(MasterUrl.localAll)
      .enableHiveSupport()
      .appName(this.getClass.getName)
      .getOrCreate()
    //    val csv = spark.read.option("header", "true").csv("hdfs://qingcheng11:9000/input/spark/sales.csv")
    //    csv.show()
    //    val csv2 = spark.read.format("csv").options(Map("header" -> "true", "inferSchema" -> "true")).load("hdfs://qingcheng11:9000/input/spark/sales.csv")
    //    csv2.show()


    //    val jsonFilePath = "hdfs://qingcheng11:9000/input/spark/teacher.json"
    //    val json1 = spark.read.json(jsonFilePath)
    //    json1.show()
    //    val json2 = spark.read.format("json").load(jsonFilePath)
    //    json2.show()


    //    val text = spark.read.textFile("hdfs://qingcheng11:9000/input/spark/README.md")
    //    text.show()
    //    val text2 = spark.read.text("hdfs://qingcheng11:9000/input/spark/README.md")
    //    text2.show()
    //    val text3 = spark.read.text("hdfs://qingcheng11:9000/input/spark/person_libsvm.txt", "hdfs://qingcheng11:9000/input/spark/README.md")
    //    text3.show()
    //    val text4 = spark.read.text("hdfs://qingcheng11:9000/input/spark/*.csv", "hdfs://qingcheng11:9000/input/spar*/*.json")
    //    text4.show(Int.MaxValue - 1)

    //        val parquetFilePath = "hdfs://qingcheng11:9000/input/spark/users.parquet"
    //        val parquet1 = spark.read.options(Map("mergeSchema"->"true")).parquet(parquetFilePath)
    //        parquet1.show()
    //    val parquet2 = spark.read.format("parquet").load(parquetFilePath)
    //    parquet2.show()


    val avroFilePath = "hdfs://qingcheng11:9000/input/spark/users.avro"
    val avro1 = spark.read.format("com.databricks.spark.avro").load(avroFilePath)
    avro1.show()


  }
}
