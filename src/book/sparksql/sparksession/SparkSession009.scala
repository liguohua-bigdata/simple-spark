package sparksql.sparksession

import book.utils.MasterUrl
import org.apache.spark.sql.SparkSession

/**
  * Created by liguohua on 17/12/2016.
  */
object SparkSession009 {
  def main(args: Array[String]): Unit = {
    //0.创建SparkSession
    val spark = SparkSession.builder
      .master(MasterUrl.localAll)
      .enableHiveSupport()
      .appName(this.getClass.getName)
      .getOrCreate()

    val parquetFilePath = "hdfs://qingcheng11:9000/input/spark/users.parquet"
    val m = Map("mergeSchema" -> "true")
    //1.读取parquet文件的第一种方式
    val parquet1 = spark.read.options(m).parquet(parquetFilePath)
    parquet1.show()

    //2.读取parquet文件的第二种方式
    val parquet2 = spark.read.format("parquet").load(parquetFilePath)
    parquet2.show()

    val outDir = "hdfs://qingcheng11:9000/output/spark/sparksession/parquet/"
    //3.写parquet文件的第一种方式
    parquet1.write.options(m).parquet(outDir + "parquet1")

    //4.写parquet文件的第二种方式
    parquet2.write.format("parquet").save(outDir + "parquet2")
  }
}
