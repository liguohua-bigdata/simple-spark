#一、SparkSession读取Mysql数据库的四种方式讲解
##1.不指定查询条件
方法原型
```
def jdbc(
    url: String, 
    table: String, 
    properties: Properties): DataFrame
    
url就是mysqlJDBC的连接url
table就是表名称
    
spark默认开启一个线程拉取所有的数据，
一方面，并行度太低，效率太低。
另一方面，如果MySQL表中的数据较大，很有可能出现OOM.
```
执行程序
```scala
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
      .appName("RDDToDataSet")
      .getOrCreate()

    //2.创建数据库连接
    val url = "jdbc:mysql://qingcheng11:3306/sparktest?user=root&password=qingcheng"
    val table = "Student"
    val prop = new Properties()
    //3.读取数据
    val jdbcMysql = spark.read.jdbc(url, table, prop)

    //4.显示结果
    jdbcMysql.show(Int.MaxValue - 1)
    //默认只有一个并行度，
    print(jdbcMysql.rdd.partitions.size)
  }
}
```
执行效果
```
+--------+------+-------+
| sutName|stuAge|stuAddr|
+--------+------+-------+
|zhangsan|    16|tianjin|
|    lisi|    18|beijing|
+--------+------+-------+

分区数=1
```



##2.指定数据库字段的范围
```
def jdbc(
    url: String,
    table: String,
    columnName: String,
    lowerBound: Long,
    upperBound: Long,
    numPartitions: Int,
    connectionProperties: Properties): DataFrame
    
url就是mysqlJDBC的连接url
table就是表名称
columnName就是需要分区的字段，这个字段在数据库中的类型必须是数字；
lowerBound就是分区的下界；
upperBound就是分区的上界；
numPartitions是分区的个数。
```
执行程序
```scala

```
执行效果
```
+--------+------+-------+
| sutName|stuAge|stuAddr|
+--------+------+-------+
|zhangsan|    16|tianjin|
|    lisi|    18|beijing|
+--------+------+-------+

分区数=5
```





##3.根据任意字段进行分区
```
def jdbc(
    url: String,
    table: String,
    predicates: Array[String],
    connectionProperties: Properties): DataFrame
    
url就是mysqlJDBC的连接url
table就是表名称
predicates分区依据，每一一个predicate就会形成一个分区，分区内的数据可能会重复。因此最后要去重。
```
执行程序
```scala
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
      .appName("RDDToDataSet")
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
```
执行效果
```
去重前显示的结果，每个predicate形成一个分区，数据可能重复
+--------+------+-------+
| sutName|stuAge|stuAddr|
+--------+------+-------+
|zhangsan|    16|tianjin|
|    lisi|    18|beijing|
|    lisi|    18|beijing|
|    lisi|    18|beijing|
+--------+------+-------+

分区数=3


去重后显示的结果，每个predicate形成一个分区，数据需要去重
+--------+------+-------+
| sutName|stuAge|stuAddr|
+--------+------+-------+
|    lisi|    18|beijing|
|zhangsan|    16|tianjin|
+--------+------+-------+

分区数=3
```





##1.不指定查询条件
```

```
执行程序
```scala

```
执行效果







