##1.SparkSession读写text文件
执行程序
```scala
package sparksql.sparksession
import book.utils.MasterUrl
import org.apache.spark.sql.SparkSession
object SparkSession006 {
  def main(args: Array[String]): Unit = {
    //0.创建SparkSession
    val spark = SparkSession.builder
      .master(MasterUrl.localAll)
      .enableHiveSupport()
      .appName(this.getClass.getName)
      .getOrCreate()

    //1.第1种读取方式read.textFile
    val text1 = spark.read.textFile("hdfs://qingcheng11:9000/input/spark/README.md")
    text1.show()

    //2.第2种读取方式read.text
    val text2 = spark.read.text("hdfs://qingcheng11:9000/input/spark/README.md")
    text2.show()

    //3.第3种读取方式read.text读取多个文件
    val text3 = spark.read.text("hdfs://qingcheng11:9000/input/spark/person_libsvm.txt",
    "hdfs://qingcheng11:9000/input/spark/README.md")
    text3.show()

    //4.第4种读取方式read.用郑总表达式匹配文件
    val text4 = spark.read.text("hdfs://qingcheng11:9000/input/spark/*.csv",
    "hdfs://qingcheng11:9000/input/spar*/*.json")
    text4.show()

    //5.第1种写出方式rdd.saveAsTextFile
    val outDir = "hdfs://qingcheng11:9000/output/spark/sparksession/"
    text1.rdd.saveAsTextFile(outDir + "saveAsTextFile")

    //6.第2种写出方式write.format("text").save()
    text1.write.format("text").save(outDir + "writerForma")
    spark.stop()
  }
}

```
执行效果
![](images/Snip20161217_5.png) 
##2.SparkSession读写csv文件
执行程序
```
package sparksql.sparksession

import book.utils.MasterUrl
import org.apache.spark.sql.SparkSession
object SparkSession007 {
  def main(args: Array[String]): Unit = {
    //0.创建SparkSession
    val spark = SparkSession.builder
      .master(MasterUrl.localAll)
      .enableHiveSupport()
      .appName(this.getClass.getName)
      .getOrCreate()
    val m = Map("header" -> "true", "inferSchema" -> "true")
    //1.读取csv的方式一
    val csv1 = spark.read.options(m).csv("hdfs://qingcheng11:9000/input/spark/sales.csv")
    csv1.show()
    //2.读取csv的方式二
    val csv2 = spark.read.format("csv").options(m)
    .load("hdfs://qingcheng11:9000/input/spark/sales.csv")
    csv2.show()

    //写出csv文件的方式一
    val outDir = "hdfs://qingcheng11:9000/output/spark/sparksession/csv/"
    csv1.write.options(m).csv(outDir + "csv1")
    //写出csv文件的方式二
    csv2.write.format("csv").options(m).save(outDir + "csv2")
  }
}
```
执行效果
![](images/Snip20161217_4.png) 


##3.SparkSession读写json文件
执行程序
```
package sparksql.sparksession

import book.utils.MasterUrl
import org.apache.spark.sql.SparkSession

/**
  * Created by liguohua on 17/12/2016.
  */
object SparkSession008 {
  def main(args: Array[String]): Unit = {
    //0.创建SparkSession
    val spark = SparkSession.builder
      .master(MasterUrl.localAll)
      .enableHiveSupport()
      .appName(this.getClass.getName)
      .getOrCreate()
    
    val jsonFilePath = "hdfs://qingcheng11:9000/input/spark/teacher.json"
    
    //1.读取json文件的第一种方式
    val json1 = spark.read.json(jsonFilePath)
    json1.show()

    //2.读取json文件的第二种方式
    val json2 = spark.read.format("json").load(jsonFilePath)
    json2.show()

    //3.写json文件的第一种方式
    val outDir = "hdfs://qingcheng11:9000/output/spark/sparksession/json/"
    json1.write.json(outDir+"json1")

    //4.写json文件的第二种方式
    json2.write.format("json").save(outDir+"json2")
  }
}
```
执行效果
![](images/Snip20161217_6.png) 
