package sparksql.sparksession

import book.utils.MasterUrl
import com.databricks.spark.avro._
import org.apache.spark.sql.SparkSession

object SparkSession0010 {
  def main(args: Array[String]): Unit = {
    //0.创建SparkSession
    val spark = SparkSession.builder
      .master(MasterUrl.localAll)
      .enableHiveSupport()
      .appName(this.getClass.getName)
      .getOrCreate()

    //1.读取avro文件的第一种方式
    val avroFilePath = "hdfs://qingcheng11:9000/input/spark/users.avro"
    val avro1 = spark.read.format("com.databricks.spark.avro").load(avroFilePath)
    avro1.show()

    //2.读取avro文件的第二种方式
    val avro2 = spark.read.avro(avroFilePath)
    avro2.show()

    val outDir = "hdfs://qingcheng11:9000/output/spark/sparksession/avro/"
    //3.写avro文件的第一种方式
    avro1.write.format("com.databricks.spark.avro").save(outDir + " avro1")

    //4.写avro文件的第二种方式
    avro2.write.avro(outDir + " avro2")
  }
}


