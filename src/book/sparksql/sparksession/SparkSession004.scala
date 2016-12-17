package book.sparksql.sparksession

import java.util.Properties

import book.utils.MasterUrl
import org.apache.spark.sql.SparkSession

object SparkSession004 {
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
    val predicates = Array[String]("stuAge > 15 and stuAge <50" ,
      "sutName = 'lisi'" ,"stuAddr like 'beijing'")
    val prop = new Properties()

    //3.读取数据
    val jdbcMysql = spark.read.jdbc(url, table, predicates, prop)

    //4.显示结果
    println("去重前显示的结果，每个predicate形成一个分区，数据可能重复")
    jdbcMysql.show()
    println("分区数=" + jdbcMysql.rdd.partitions.size)

    //5.去重后显示结果
    println("去重后显示的结果，每个predicate形成一个分区，数据需要去重")
    jdbcMysql.distinct().show()
    println("分区数=" + jdbcMysql.rdd.partitions.size)
  }
}
