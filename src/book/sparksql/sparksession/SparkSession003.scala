package book.sparksql.sparksession

import java.util.Properties

import book.utils.MasterUrl
import org.apache.spark.sql.SparkSession

/**
  * https://www.iteblog.com/archives/1560
  */
object SparkSession003 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master(MasterUrl.localAll)
      .enableHiveSupport()
      .appName("RDDToDataSet")
      .getOrCreate()

    /**
      * def jdbc(url: String, table: String, properties: Properties): DataFrame
      */
    val url = "jdbc:mysql://qingcheng11:3306/sparktest?user=root&password=qingcheng"
    val table = "Student"
    val prop = new Properties()

    val jdbcMysql1 = spark.read.jdbc(url, table, prop)
    jdbcMysql1.show(Int.MaxValue - 1)
    print(jdbcMysql1.rdd.partitions.size)


    /**
      * def jdbc(
      * url: String,
      * table: String,
      * columnName: String,
      * lowerBound: Long,
      * upperBound: Long,
      * numPartitions: Int,
      * connectionProperties: Properties): DataFrame
      */
    val collum = "stuAge"
    val lowerBound = 1
    val upperBound = 100000
    val numPartitions = 5

    val jdbcMysql2 = spark.read.jdbc(url, table, collum, lowerBound, upperBound, numPartitions, prop)
    jdbcMysql2.show(Int.MaxValue - 1)
    print(jdbcMysql2.rdd.partitions.size)


    /**
      * def jdbc(
      * url: String,
      * table: String,
      * predicates: Array[String],
      * connectionProperties: Properties): DataFrame
      */

  }
}
