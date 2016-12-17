package sparksql.sparksession

import book.utils.MasterUrl
import com.databricks.spark.avro._
import org.apache.spark.sql.SparkSession

/*
<dependency>
  <groupId>com.databricks</groupId>
  <artifactId>spark-avro_2.10</artifactId>
  <version>3.1.0</version>
</dependency>

<dependency>
  <groupId>com.databricks</groupId>
  <artifactId>spark-avro_2.11</artifactId>
  <version>3.1.0</version>
</dependency>
 */
object SparkSession0010 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master(MasterUrl.localAll)
      .enableHiveSupport()
      .appName(this.getClass.getName)
      .getOrCreate()
    val avroFilePath = "hdfs://qingcheng11:9000/input/spark/users.avro"
    val avro1 = spark.read.format("com.databricks.spark.avro").load(avroFilePath)
    avro1.show()
    val avro2 = spark.read.avro(avroFilePath)
    avro2.show()

    val avroOutPath = "hdfs://qingcheng11:9000/output/spark/"
    avro1.write.format("com.databricks.spark.avro").save(avroOutPath + "file1.avro")
    avro2.write.avro(avroOutPath + "file2.avro")
  }
}


