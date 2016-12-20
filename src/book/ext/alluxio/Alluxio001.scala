package ext.alluxio
import org.apache.spark.{SparkConf, SparkContext}

object Alluxio001 {
  def main(args: Array[String]) {
    //1.创建spark执行环境
    val conf = new SparkConf().setAppName(this.getClass.getName)
      .setMaster("local[*]")
    val spark = new SparkContext(conf)

    //2.读取alluxio上的文件
    val rdd = spark.textFile("alluxio://qingcheng11:19998/input/spark/README.md")

    //3.显示文件内容
    rdd.collect().foreach(print(_))

    //4.关闭spark执行上下文
    spark.stop()
  }
}