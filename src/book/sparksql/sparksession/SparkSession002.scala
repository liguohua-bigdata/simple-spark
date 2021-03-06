package book.sparksql.sparksession

import java.util.Properties

import book.utils.MasterUrl
import org.apache.spark.sql.SparkSession

object SparkSession002 {
  def main(args: Array[String]): Unit = {
    //1.创建SparkSession
    val spark = SparkSession.builder
      .master(MasterUrl.localAll)
      .enableHiveSupport()
      .appName(this.getClass.getName)
      .getOrCreate()

    //2.创建数据库连接
    val url = "jdbc:mysql://qingcheng11:3306/sparktest?user=root&password=qingcheng"
    val table = "Student"
    val prop = new Properties()
    //3.读取数据
    val jdbcMysql = spark.read.jdbc(url, table, prop)

    //4.显示结果
    jdbcMysql.show()
    println("分区数=" + jdbcMysql.rdd.partitions.size)
  }
}
