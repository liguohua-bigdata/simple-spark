package book.sparksql.sparksession

import org.apache.spark.sql.SparkSession

/**
  * Created by liguohua on 07/12/2016.
  */
object DataSetWordCount {
  def main(args: Array[String]) {

    //1.创建SparkSession
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("DataSetWordCount")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    //2.读取HDFS上的文件
    val data = spark.read.text("hdfs://qingcheng12:9000/input/spark/README.md").as[String]
    data.show()

    //3.进行wordcount操作
    val words = data.flatMap(_.split("\\s+")).filter(_.nonEmpty)
    val groupedWords = words.groupByKey(_.toLowerCase)
    groupedWords.count().show()
    //4.关闭SparkSession
    spark.stop()

  }

}
