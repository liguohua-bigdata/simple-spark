package book.sparksql.sparksession

import book.utils.MasterUrl
import org.apache.spark.sql.SparkSession

object SparkSession005 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkSession
    val spark = SparkSession.builder
      .master(MasterUrl.localAll)
      .enableHiveSupport()
      .appName(this.getClass.getName)
      .getOrCreate()

    //2.读取数据
    val m = Map(
      "url" -> "jdbc:mysql://qingcheng11:3306/sparktest?user=root&password=qingcheng",
      "dbtable" -> "Student",
      "partitionColumn" -> "stuAge",
      "lowerBound" -> "1",
      "upperBound" -> "100000",
      "numPartitions" -> "5"
    )
    val jdbcMysql = spark.read.format("jdbc").options(m).load()

    //3.显示结果
    jdbcMysql.show()
    println("分区数=" + jdbcMysql.rdd.partitions.size)
  }
}
