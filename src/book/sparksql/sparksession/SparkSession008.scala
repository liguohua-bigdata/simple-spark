package sparksql.sparksession

import book.utils.MasterUrl
import org.apache.spark.sql.SparkSession

/**
  * Created by liguohua on 17/12/2016.
  */
object SparkSession008 {
  def main(args: Array[String]): Unit = {
    //0.创建SparkSession
    val spark = SparkSession.builder
      .master(MasterUrl.localAll)
      .enableHiveSupport()
      .appName(this.getClass.getName)
      .getOrCreate()

    val jsonFilePath = "hdfs://qingcheng11:9000/input/spark/teacher.json"

    //1.读取json文件的第一种方式
    val json1 = spark.read.json(jsonFilePath)
    json1.show()

    //2.读取json文件的第二种方式
    val json2 = spark.read.format("json").load(jsonFilePath)
    json2.show()

    //3.写json文件的第一种方式
    val outDir = "hdfs://qingcheng11:9000/output/spark/sparksession/json/"
    json1.write.json(outDir+"json1")

    //4.写json文件的第二种方式
    json2.write.format("json").save(outDir+"json2")
  }
}
