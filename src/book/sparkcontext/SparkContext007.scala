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

    val lines = spark.parallelize(Seq("hello world", "nice to see you"))

    val lines2=spark.runJob(lines,(t: TaskContext, i: Iterator[String]) => {
       while (i.hasNext){
          i.next.toUpperCase
       }
    })

    lines2.foreach(println(_))

    //6.关闭sparkcontext
    if (!spark.isStopped) {
      spark.stop()
    }
  }
}
