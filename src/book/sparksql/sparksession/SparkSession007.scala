package sparksql.sparksession

import book.utils.MasterUrl
import org.apache.spark.sql.SparkSession
object SparkSession007 {
  def main(args: Array[String]): Unit = {
    //0.创建SparkSession
    val spark = SparkSession.builder
      .master(MasterUrl.localAll)
      .enableHiveSupport()
      .appName(this.getClass.getName)
      .getOrCreate()
    val m = Map("header" -> "true", "inferSchema" -> "true")
    //1.读取csv的方式一
    val csv1 = spark.read.options(m).csv("hdfs://qingcheng11:9000/input/spark/sales.csv")
    csv1.show()
    //2.读取csv的方式二
    val csv2 = spark.read.format("csv").options(m).load("hdfs://qingcheng11:9000/input/spark/sales.csv")
    csv2.show()

    //写出csv文件的方式一
    val outDir = "hdfs://qingcheng11:9000/output/spark/sparksession/csv/"
    csv1.write.options(m).csv(outDir + "csv1")
    //写出csv文件的方式二
    csv2.write.format("csv").options(m).save(outDir + "csv2")
  }
}
