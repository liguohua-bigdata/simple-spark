#一、SparkSession读取Mysql数据库的四种方式讲解


#二、SparkSession常用API
##1.不指定查询条件
方法原型
```
def jdbc(url: String, table: String, properties: Properties): DataFrame
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



##1.不指定查询条件
```

```
执行程序
```scala

```
执行效果




##1.不指定查询条件
```

```
执行程序
```scala

```
执行效果




##1.不指定查询条件
```

```
执行程序
```scala

```
执行效果







