package sparksql.sparksession

import book.utils.MasterUrl
import org.apache.spark.sql.SparkSession

object SparkSession006 {
  def main(args: Array[String]): Unit = {
    //0.创建SparkSession
    val spark = SparkSession.builder
      .master(MasterUrl.localAll)
      .enableHiveSupport()
      .appName(this.getClass.getName)
      .getOrCreate()

    //1.第1种读取方式read.textFile
    val text1 = spark.read.textFile("hdfs://qingcheng11:9000/input/spark/README.md")
    text1.show()

    //2.第2种读取方式read.text
    val text2 = spark.read.text("hdfs://qingcheng11:9000/input/spark/README.md")
    text2.show()

    //3.第3种读取方式read.text读取多个文件
    val text3 = spark.read.text("hdfs://qingcheng11:9000/input/spark/person_libsvm.txt", "hdfs://qingcheng11:9000/input/spark/README.md")
    text3.show()

    //4.第4种读取方式read.用郑总表达式匹配文件
    val text4 = spark.read.text("hdfs://qingcheng11:9000/input/spark/*.csv", "hdfs://qingcheng11:9000/input/spar*/*.json")
    text4.show()

    //5.第1种写出方式rdd.saveAsTextFile
    val outDir = "hdfs://qingcheng11:9000/output/spark/sparksession/"
    text1.rdd.saveAsTextFile(outDir + "saveAsTextFile")

    //6.第2种写出方式write.format("text").save()
    text1.write.format("text").save(outDir + "writerForma")
    spark.stop()
  }
}
