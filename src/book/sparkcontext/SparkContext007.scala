package book.sparkcontext

import org.apache.spark.{SparkConf, SparkContext, TaskContext}

object SparkContext007 {
  def main(args: Array[String]): Unit = {
    //1.创建配置文件
    val sparkConf = new SparkConf()
    sparkConf.setAppName(this.getClass.getName)
    sparkConf.setMaster("local")
    //2.创建sparkContext
    val spark = new SparkContext(sparkConf)

    //3.定义数据
    val lines1 = spark.parallelize(Seq("hello world", "nice to see you"), 2)

    //4.执行runjob操作
    val lines2 = spark.runJob(lines1, (t: TaskContext, i: Iterator[String]) => {
      var str = ""
      while (i.hasNext) {
        str = str + i.next.toUpperCase
      }
      str
    })
    //5.显示结果
    lines2.foreach(println(_))

    //6.关闭sparkcontext
    if (!spark.isStopped) {
      spark.stop()
    }
  }
}
