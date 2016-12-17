package book.sparksql.sparksession

import java.util.Properties

import book.utils.MasterUrl
import org.apache.spark.sql.SparkSession

object SparkSession002 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master(MasterUrl.localAll)
      .enableHiveSupport()
      .appName("RDDToDataSet")
      .getOrCreate()
    val url = "jdbc:mysql://qingcheng11:3306/sparktest?user=root&password=qingcheng"
    val table = "Student"
    val prop = new Properties()
    val jdbcMysql = spark.read.jdbc(url, table, prop)
    jdbcMysql.show(Int.MaxValue - 1)
    //默认只有一个并行度，
    print(jdbcMysql.rdd.partitions.size)
  }
}
