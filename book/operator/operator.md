##一、spark中操作符的写法
```
package operator

import org.apache.spark.{SparkConf, SparkContext}

object Operator001 {
  def main(args: Array[String]) {
    //1.创建spark执行环境
    val conf = new SparkConf().setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val spark = new SparkContext(conf)

    //2.准备测试数据
    val rdd = spark.parallelize(Seq("spark","hadoop","flink","alluxio"))
    //3.使用简便形式
    val rdd1=rdd.map(_.toUpperCase)
    rdd1.collect().foreach(println(_))
    //4.还原参数
    val rdd2=rdd.map(w=>w.toUpperCase)
    rdd2.collect().foreach(println(_))

    //5.还原参数的类型
    val rdd3=rdd.map((w:String)=>{w.toUpperCase})
    rdd3.collect().foreach(println(_))

    //6.使用全局对象
    object MyFunctions{
      def mapToUpperFun(s: String): String = {s.toUpperCase()}
    }
    val rdd4=rdd.map(MyFunctions.mapToUpperFun(_))
    rdd4.collect().foreach(println(_))

    //6.使用全局对象,并使用全局对象中定义的字段
    object MyFunctions2{
      val field = "Hello"
      def mapToUpperFun(s: String): String = {field+s.toUpperCase()}
    }
    val rdd5=rdd.map(MyFunctions2.mapToUpperFun(_))
    rdd5.collect().foreach(println(_))

    //7.关闭spark执行上下文
    spark.stop()
  }
}
```
